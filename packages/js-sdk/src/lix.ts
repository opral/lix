import { fetchTransport } from "./http-transport.js";
import { invalidArgument } from "./errors.js";
import type {
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
	ExecuteOptions,
	ExecuteResult,
	ExecuteBatchResult,
	CommitReceipt,
	StatementResult,
	LixBatchOptions,
	LixBatchStatement,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	OpenAnotherSessionOptions,
	LixOpenReport,
	ReplicaRecoverySource,
	ReplicaRecoveryExport,
	ReplicaRecoveryReceipt,
	SqlParam,
	ResultArrayRow,
	ResultObjectRow,
	ResultRow,
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
const observationFinalizer = new FinalizationRegistry<ObservationLifecycle>(
	(lifecycle) => lifecycle.stop(),
);
const hostedCreators = new WeakMap<
	Lix,
	(
		server: () => Promise<
			import("./binding-types.js").HostedServerBindingOptions
		>,
	) => Promise<import("./types.js").HostedLix>
>();

/** @internal Used by createLix without adding another method to Lix. */
export function createHostedFromLix(
	lix: Lix,
	server: () => Promise<
		import("./binding-types.js").HostedServerBindingOptions
	>,
) {
	const create = hostedCreators.get(lix);
	if (!create)
		throw new TypeError("createLix() from must be an open local Lix");
	return create(server);
}

export class Lix {
	readonly #openReport: LixOpenReport | undefined;
	/** Immutable facts about this handle's successful opening. */
	get openReport(): LixOpenReport {
		if (!this.#openReport)
			throw new Error("Lix binding did not provide an open report");
		return this.#openReport;
	}
	private closePromise: Promise<void> | undefined;
	readonly #activeBranchListeners = new Set<() => void>();
	readonly #inFlightOperations = new Set<Promise<unknown>>();
	readonly #observations = new Map<
		number,
		{ lifecycle: ObservationLifecycle; unregisterToken: object }
	>();
	readonly #observationDrains = new Set<Promise<void>>();
	readonly #snapshotExports = new Set<{ cancel(): Promise<void> }>();
	#nextObservationId = 0;
	#transactionsOpening = 0;
	#activeTransactions = 0;
	#acceptingOperations = true;

	constructor(
		private readonly binding: LixBinding,
		private readonly flushTelemetry?: () => void | Promise<void>,
	) {
		hostedCreators.set(this, (server) =>
			this.#runOperation(async () => {
				if (!binding.createHosted)
					throw new TypeError("createLix() from requires a local Lix");
				return binding.createHosted(await server());
			}),
		);
		const report = binding.openReport?.();
		this.#openReport = report
			? Object.freeze({
					...report,
					migrations: Object.freeze(
						report.migrations.map((migration) =>
							Object.freeze({ ...migration }),
						),
					),
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
			async () =>
				new Lix(
					await this.binding.openAnotherSession(options),
					this.flushTelemetry,
				),
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
	): Promise<ExecuteBatchResult<ResultArrayRow>>;
	executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions & { rowMode?: "object" },
	): Promise<ExecuteBatchResult<ResultObjectRow>>;
	executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<ExecuteBatchResult<ResultRow>>;
	async executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<ExecuteBatchResult<ResultRow>> {
		const normalizedStatements = normalizeBatchStatements(statements, options);
		const { rowMode = "object", ...bindingOptions } = options ?? {};
		return this.#runOperation(async () => {
			const results = await this.binding.executeBatch(
				normalizedStatements,
				bindingOptions,
			);
			return {
				results: results.results.map((result) =>
					wrapExecuteBatchResult(result, rowMode),
				),
				commit: results.commit ?? null,
			};
		});
	}

	observe(
		sql: string,
		params: SqlParam[] = [],
		options: { signal?: AbortSignal } = {},
	): AsyncIterableIterator<ObserveEvent> {
		assertSqlArgs("observe", "lix", sql, params);
		const observationId = ++this.#nextObservationId;
		const unregisterToken = {};
		const lifecycle = new ObservationLifecycle(
			this.#runOperation(() =>
				this.binding.observe(
					sql,
					params.map((param, index) =>
						toNativeValue(normalizeParam(param, index)),
					),
				),
			),
			(drain) => {
				observationFinalizer.unregister(unregisterToken);
				this.#observations.delete(observationId);
				this.#observationDrains.add(drain);
				void drain.then(
					() => this.#observationDrains.delete(drain),
					() => this.#observationDrains.delete(drain),
				);
			},
			options.signal,
		);
		const events = new Observation(lifecycle);
		observationFinalizer.register(events, lifecycle, unregisterToken);
		if (this.#acceptingOperations && !options.signal?.aborted)
			this.#observations.set(observationId, { lifecycle, unregisterToken });
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

	/** Lists preserved generations belonging to this local repository. */
	async replicaRecoverySources(): Promise<ReplicaRecoverySource[]> {
		return this.#runOperation(() => this.binding.replicaRecoverySources());
	}

	/** Exports retained work without deleting or changing its source. */
	async exportReplicaRecovery(id: string): Promise<ReplicaRecoveryExport> {
		return this.#runOperation(() => this.binding.exportReplicaRecovery(id));
	}

	/** Restores supported rows onto separate recovery branches; preserves the source. */
	async recoverReplica(id: string): Promise<ReplicaRecoveryReceipt> {
		return this.#runOperation(() => this.binding.recoverReplica(id));
	}

	/** Explicitly hydrates retained-source recovery dependencies; does not start sync. */
	async recoverReplicaWithServer(
		id: string,
		server: import("./types.js").LixServerOptions,
	): Promise<ReplicaRecoveryReceipt> {
		const entries = (headers: HeadersInit | undefined): [string, string][] => {
			const result: [string, string][] = [];
			new Headers(headers).forEach((value, key) => result.push([key, value]));
			return result;
		};
		return this.#runOperation(() =>
			this.binding.recoverReplicaWithServer(id, {
				url: new URL(server.url).toString(),
				headers:
					typeof server.headers === "function" ? [] : entries(server.headers),
				headerProvider:
					typeof server.headers === "function"
						? async () =>
								entries(await (server.headers as () => Promise<HeadersInit>)())
						: undefined,
				transport: server.fetch ? fetchTransport(server.fetch) : undefined,
			}),
		);
	}

	/** Local worker health; independent of whether a warm SQL read succeeds. */
	async syncHealth(): Promise<import("./types.js").SyncHealth> {
		return this.binding.syncHealth();
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

	/** Streams this handle's state. Local partial replicas include their cached
	 * inputs and pending edits; remote handles export the complete authority. */
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
					resolveBinding(this.binding.exportSnapshot());
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
			for (const {
				lifecycle,
				unregisterToken,
			} of this.#observations.values()) {
				observationFinalizer.unregister(unregisterToken);
				lifecycle.stop();
			}
			this.#observations.clear();
			const observationDrains = [...this.#observationDrains];
			this.closePromise = (async () => {
				await Promise.allSettled(observationDrains);
				await Promise.allSettled(
					[...this.#snapshotExports].map((snapshot) => snapshot.cancel()),
				);
				await Promise.allSettled([...this.#inFlightOperations]);
				const results = await Promise.allSettled([
					Promise.resolve().then(() => this.binding.close()),
				]);
				const failures: unknown[] = results.flatMap((result) =>
					result.status === "rejected" ? [result.reason] : [],
				);
				try {
					await this.binding.flushTelemetry?.();
				} catch (error) {
					failures.push(error);
				}
				try {
					await this.flushTelemetry?.();
				} catch (error) {
					failures.push(error);
				}
				this.#activeBranchListeners.clear();
				if (failures.length === 1) throw failures[0];
				if (failures.length > 1) {
					throw new AggregateError(
						failures,
						"Lix close or telemetry export failed",
					);
				}
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

class Observation implements AsyncIterableIterator<ObserveEvent> {
	constructor(private readonly lifecycle: ObservationLifecycle) {}

	[Symbol.asyncIterator](): AsyncIterableIterator<ObserveEvent> {
		return this;
	}

	next(): Promise<IteratorResult<ObserveEvent>> {
		return this.lifecycle.next();
	}

	return(): Promise<IteratorResult<ObserveEvent>> {
		return this.lifecycle.return();
	}
}

class ObservationLifecycle {
	private readonly stopped = new Set<() => void>();
	private readonly abort = () => this.stop();
	private readonly setup: { error?: unknown } = {};
	private closed = false;
	private bindingClosePromise: Promise<void> | undefined;
	private drainPromise: Promise<void> | undefined;
	private readonly observeBinding: Promise<ObserveEventsBinding | undefined>;

	constructor(
		observeBinding: Promise<ObserveEventsBinding>,
		private readonly onClose: (drain: Promise<void>) => void = () => undefined,
		private readonly signal?: AbortSignal,
	) {
		const setup = this.setup;
		this.observeBinding = observeBinding.catch((error: unknown) => {
			setup.error = error;
			return undefined;
		});
		if (signal?.aborted) this.stop();
		else signal?.addEventListener("abort", this.abort, { once: true });
	}

	async next(): Promise<IteratorResult<ObserveEvent>> {
		if (this.closed || this.signal?.aborted) {
			this.stop();
			return { done: true, value: undefined };
		}
		let stop!: () => void;
		const stopped = new Promise<undefined>((resolve) => {
			stop = () => resolve(undefined);
			this.stopped.add(stop);
		});
		try {
			const pendingRead = (async () => {
				const binding = await this.observeBinding;
				if (this.closed) return undefined;
				if (binding === undefined) throw this.setup.error;
				return await binding.next();
			})();
			const event = await Promise.race([pendingRead, stopped]);
			if (this.closed || event == null) {
				this.stop();
				return { done: true, value: undefined };
			}
			return {
				done: false,
				value: {
					sequence: event.sequence,
					mutationSequence: event.mutationSequence,
					result: wrapExecuteResult(event.rows),
				},
			};
		} catch (error) {
			const wasClosed = this.closed;
			this.stop();
			if (wasClosed) return { done: true, value: undefined };
			throw error;
		} finally {
			this.stopped.delete(stop);
		}
	}

	async return(): Promise<IteratorResult<ObserveEvent>> {
		this.stop();
		await this.drainPromise;
		return { done: true, value: undefined };
	}

	stop(): void {
		if (this.closed) return;
		this.closed = true;
		this.signal?.removeEventListener("abort", this.abort);
		for (const stop of this.stopped) stop();
		this.stopped.clear();
		this.bindingClosePromise ??= this.observeBinding
			.then((binding) => binding?.close())
			.then(
				() => undefined,
				() => undefined,
			);
		// The binding's close is the observer resource barrier. Waiting directly on
		// a `next()` promise can hang forever for bindings that don't reject pending reads.
		this.drainPromise ??= this.bindingClosePromise;
		this.onClose(this.drainPromise);
	}
}

export class LixTransaction {
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
	): Promise<StatementResult<ResultArrayRow>>;
	execute<TRow extends object = ResultObjectRow>(
		sql: string,
		params?: SqlParam[],
		options?: ExecuteOptions & { rowMode?: "object" },
	): Promise<StatementResult<TRow>>;
	execute(
		sql: string,
		params: SqlParam[] | undefined,
		options?: ExecuteOptions,
	): Promise<StatementResult<ResultRow>>;
	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<StatementResult<ResultRow>> {
		if (this.finished) throw transactionClosedError();
		assertExecuteArgs("lixTransaction", sql, params, options);
		const { rowMode = "object", ...bindingOptions } = options ?? {};
		const { commit: _commit, ...statement } = wrapExecuteResult(
			await this.binding
				.execute(
					sql,
					params.map((param, index) =>
						toNativeValue(normalizeParam(param, index)),
					),
					bindingOptions,
				)
				.catch((error: unknown) => {
					if ((error as { code?: string })?.code === "LIX_TRANSACTION_LOST") {
						this.finished = true;
						transactionFinalizer.unregister(this);
						this.onFinish();
					}
					throw error;
				}),
			rowMode,
		);
		return statement;
	}

	async commit(): Promise<CommitReceipt> {
		return (await this.finish("transaction.commit"))!;
	}

	async rollback(): Promise<void> {
		await this.finish("transaction.rollback");
	}

	private async finish(
		kind: "transaction.commit" | "transaction.rollback",
	): Promise<CommitReceipt | undefined> {
		if (this.finished) throw transactionClosedError();
		// The first terminal call owns the handle immediately. In particular,
		// a concurrent rollback must never report a pending commit's success.
		this.finished = true;
		try {
			if (kind === "transaction.commit") {
				const receipt = await this.binding.commit();
				return { commit: receipt.commit ?? null };
			}
			await this.binding.rollback();
		} finally {
			// Keep the parent transaction lease until the binding settles. A
			// terminal call consumes the handle even when it reports an error.
			transactionFinalizer.unregister(this);
			this.onFinish();
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
		options.maxAutoCommitRetries !== undefined &&
		(!Number.isInteger(options.maxAutoCommitRetries) ||
			options.maxAutoCommitRetries < 0 ||
			options.maxAutoCommitRetries > 0xffff_ffff)
	) {
		throw invalidArgument(
			"execute",
			"options.maxAutoCommitRetries",
			"integer between 0 and 4294967295",
			typeof options.maxAutoCommitRetries,
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
		options.maxAutoCommitRetries !== undefined &&
		(!Number.isInteger(options.maxAutoCommitRetries) ||
			options.maxAutoCommitRetries < 0 ||
			options.maxAutoCommitRetries > 0xffff_ffff)
	) {
		throw invalidArgument(
			"executeBatch",
			"options.maxAutoCommitRetries",
			"integer between 0 and 4294967295",
			typeof options.maxAutoCommitRetries,
		);
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
