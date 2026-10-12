import { fetchTransport } from "./http-transport.js";
import { invalidArgument } from "./errors.js";
import type {
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "./binding-types.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import {
	LixRowStream,
	LixSnapshot,
	closedError,
	executeOptions,
	nativeParamSets,
	nativeParams,
	query,
	queryOne,
	stream,
	throwIfAborted,
	withAbort,
	type ReaderContext,
} from "./reader.js";
import { normalizeParam, toNativeValue } from "./value.js";
import type {
	CallOptions,
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteManyOptions,
	ExecuteOptions,
	ExecuteResult,
	CommitReceipt,
	StatementResult,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	OpenAnotherSessionOptions,
	QueryOptions,
	Reader,
	LixOpenHostProfile,
	LixOpenReport,
	ReplicaRecoverySource,
	ReplicaRecoveryExport,
	ReplicaRecoveryReceipt,
	Row,
	RowStream,
	Snapshot,
	SqlParam,
	StreamOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
	TransactionOptions,
	TransactionResult,
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

function freezeHostProfile(profile: LixOpenHostProfile): LixOpenHostProfile {
	return Object.freeze({
		...profile,
		wasm: Object.freeze({ ...profile.wasm }),
		componentCompiler: Object.freeze({ ...profile.componentCompiler }),
		...(profile.provider
			? {
					provider: Object.freeze({
						...profile.provider,
						...(profile.provider.opfs
							? { opfs: Object.freeze({ ...profile.provider.opfs }) }
							: {}),
					}),
				}
			: {}),
	});
}

export class Lix implements Reader {
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
	readonly #queryStreams = new Set<LixRowStream<object>>();
	readonly #snapshots = new Set<LixSnapshot>();
	readonly #reader: ReaderContext = {
		receiver: "lix",
		run: (operation) => this.#runOperation(operation),
		binding: () => this.binding,
		adopt: (stream) => this.#adoptStream(stream),
	};
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
					...(report.hostProfile
						? { hostProfile: freezeHostProfile(report.hostProfile) }
						: {}),
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

	/**
	 * Reads the latest committed state. Reads only: statements that write
	 * reject with `LIX_ERROR_READ_ONLY`. The result is buffered and capped at
	 * 64 MiB of row data (`maxBytes`); beyond it the call rejects with
	 * `LIX_RESULT_TOO_LARGE`. Use `stream()` for large reads.
	 */
	query<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: QueryOptions,
	): Promise<TRow[]> {
		return query<TRow>(this.#reader, sql, params, options);
	}

	/** At most one row: `undefined` when there is none; rejects `LIX_NOT_UNIQUE` when there are more. */
	queryOne<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: CallOptions,
	): Promise<TRow | undefined> {
		return queryOne<TRow>(this.#reader, sql, params, options);
	}

	/**
	 * Streams a read of the latest committed state with bounded memory. Resolves
	 * once the statement is planned and its first page has arrived, so planning
	 * errors reject here. No size cap and no deadline; the stream holds its
	 * state until it ends or is closed (`close()`, `break`, `await using`,
	 * aborting `signal`, or closing this Lix).
	 */
	stream<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: StreamOptions,
	): Promise<RowStream<TRow>> {
		return stream<TRow>(this.#reader, sql, params, options);
	}

	/**
	 * Runs `fn` with a snapshot: every read in `fn` sees one state, and streams
	 * may be open concurrently. The snapshot closes when `fn` settles. Aborting
	 * `signal` closes it early; `fn`'s pending reads then reject.
	 */
	async read<R>(
		fn: (snapshot: Snapshot) => Promise<R> | R,
		options: CallOptions = {},
	): Promise<R> {
		if (typeof fn !== "function")
			throw invalidArgument("read", "fn", "function", typeof fn);
		const snapshot = await this.snapshot(options);
		const abort = () => void snapshot.close().catch(() => undefined);
		options.signal?.addEventListener("abort", abort, { once: true });
		try {
			const value = await fn(snapshot);
			throwIfAborted(options.signal);
			return value;
		} catch (error) {
			throwIfAborted(options.signal);
			throw error;
		} finally {
			options.signal?.removeEventListener("abort", abort);
			await snapshot.close();
		}
	}

	/** Pins the current state for reads that outlive one callback. Close it when done. */
	async snapshot(options: CallOptions = {}): Promise<Snapshot> {
		throwIfAborted(options.signal);
		const opening = this.#runOperation(() => this.binding.snapshot());
		const binding = await withAbort(opening, options.signal).catch((error: unknown) => {
			void opening.then(
				(opened) => opened.close(),
				() => undefined,
			);
			throw error;
		});
		const snapshot: LixSnapshot = new LixSnapshot(binding, () =>
			this.#snapshots.delete(snapshot),
		);
		if (!this.#acceptingOperations) {
			await snapshot.close();
			throw closedError();
		}
		this.#snapshots.add(snapshot);
		return snapshot;
	}

	/**
	 * Runs any single statement: writes, DDL or reads. Resolves with the rows
	 * (a read's result or `RETURNING` rows), `rowsAffected`, and the commit a
	 * write published. Rows are buffered under the same 64 MiB cap as
	 * `query()`. For reads, `query()` is the idiomatic form and never writes.
	 */
	async execute<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options: ExecuteOptions = {},
	): Promise<ExecuteResult<TRow>> {
		assertSql("execute", "lix", sql);
		const native = nativeParams("execute", "lix", params);
		const bindingOptions = executeOptions("execute", "lix", options);
		throwIfAborted(options.signal);
		return wrapExecuteResult(
			await this.#runOperation(() =>
				this.binding.execute(sql, native, bindingOptions),
			),
		) as ExecuteResult<TRow>;
	}

	/**
	 * Runs one statement once per parameter set, atomically and in one round
	 * trip. Resolves with the total rows affected. The statement must not
	 * return rows (no reads, no `RETURNING`).
	 */
	async executeMany(
		sql: string,
		paramSets: readonly (readonly SqlParam[])[],
		options: ExecuteManyOptions = {},
	): Promise<{ rowsAffected: number }> {
		assertSql("executeMany", "lix", sql);
		const native = nativeParamSets("lix", paramSets);
		const bindingOptions = executeOptions("executeMany", "lix", options);
		throwIfAborted(options.signal);
		const { rowsAffected } = await this.#runOperation(() =>
			this.binding.executeMany(sql, native, bindingOptions),
		);
		return { rowsAffected };
	}

	#adoptStream(stream: LixRowStream<object>): void {
		if (!this.#acceptingOperations) {
			void stream.fail(closedError());
			return;
		}
		this.#queryStreams.add(stream);
		stream.onClose(() => this.#queryStreams.delete(stream));
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
			(signal) =>
				this.#runOperation(() =>
					this.binding.observe(
						sql,
						params.map((param, index) =>
							toNativeValue(normalizeParam(param, index)),
						),
						{ signal },
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

	/**
	 * Runs `fn` in an explicit transaction and commits it.
	 *
	 * A commit fails with `LIX_TRANSACTION_CONFLICT` only when a concurrent
	 * commit changed rows this transaction read for a decision or writes
	 * itself. On such a conflict the transaction is rolled back and `fn` runs
	 * again on a fresh transaction, up to `maxRetries` times (default 3; `0`
	 * never reruns). Any other error rolls back and rejects immediately.
	 *
	 * `fn` must not commit or roll back `tx` itself, and side effects outside
	 * the transaction must tolerate a rerun. Aborting `signal` always rolls
	 * back: the call rejects with the signal's reason and never commits.
	 */
	async transaction<T>(
		fn: (tx: LixTransaction) => Promise<T> | T,
		options: TransactionOptions = {},
	): Promise<TransactionResult<T>> {
		if (typeof fn !== "function") {
			throw invalidArgument("transaction", "fn", "function", typeof fn);
		}
		const maxRetries = options.maxRetries ?? DEFAULT_TRANSACTION_RETRIES;
		if (!Number.isSafeInteger(maxRetries) || maxRetries < 0) {
			throw invalidArgument(
				"transaction",
				"options.maxRetries",
				"non-negative integer",
				String(maxRetries),
			);
		}
		const signal = options.signal;
		for (let retries = 0; ; retries += 1) {
			throwIfAborted(signal);
			const tx = await this.beginTransaction();
			try {
				const value = await fn(tx);
				throwIfAborted(signal);
				const { commit } = await tx.commit();
				return { value, commit, retries };
			} catch (error) {
				await tx.rollback().catch(() => undefined);
				throwIfAborted(signal);
				if (isTransactionConflict(error) && retries < maxRetries) continue;
				if (isTransactionConflict(error))
					annotateTransactionRetries(error, retries, maxRetries);
				throw error;
			}
		}
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

	/**
	 * Prepare the bounded baseline graph frontier needed by local writes on a
	 * partial replica. This does not mutate or publish SQL data; file content,
	 * plugins, and other query-specific inputs remain demand-driven.
	 */
	async prepareOfflineEditing(): Promise<void> {
		const prepare = this.binding.prepareOfflineEditing;
		if (typeof prepare !== "function") {
			const error = new Error(
				"offline editing preparation is unavailable for this Lix binding",
			) as Error & { code: string };
			error.code = "LIX_SYNC_MODE_MISMATCH";
			throw error;
		}
		return this.#runOperation(() => prepare.call(this.binding));
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
			const queryStreams = [...this.#queryStreams].map((stream) =>
				stream.fail(closedError()),
			);
			this.#queryStreams.clear();
			const snapshots = [...this.#snapshots].map((snapshot) => snapshot.close());
			this.#snapshots.clear();
			this.closePromise = (async () => {
				await Promise.allSettled(observationDrains);
				await Promise.allSettled(queryStreams);
				await Promise.allSettled(snapshots);
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
		throw closedError();
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
	private readonly setupController = new AbortController();
	private readonly setup: { error?: unknown } = {};
	private closed = false;
	private bindingClosePromise: Promise<void> | undefined;
	private drainPromise: Promise<void> | undefined;
	private readonly observeBinding: Promise<ObserveEventsBinding | undefined>;

	constructor(
		observeBinding: (signal: AbortSignal) => Promise<ObserveEventsBinding>,
		private readonly onClose: (drain: Promise<void>) => void = () => undefined,
		private readonly signal?: AbortSignal,
	) {
		const setup = this.setup;
		let observeBindingPromise: Promise<ObserveEventsBinding | undefined>;
		if (signal?.aborted) {
			observeBindingPromise = Promise.resolve(undefined);
		} else {
			try {
				observeBindingPromise = Promise.resolve(observeBinding(this.setupController.signal));
			} catch (error) {
				observeBindingPromise = Promise.reject(error);
			}
		}
		this.observeBinding = observeBindingPromise.catch((error: unknown) => {
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
		this.setupController.abort();
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

const DEFAULT_TRANSACTION_RETRIES = 3;

function isTransactionConflict(error: unknown): boolean {
	return (
		typeof error === "object" &&
		error !== null &&
		(error as { code?: unknown }).code === "LIX_TRANSACTION_CONFLICT"
	);
}

function annotateTransactionRetries(
	error: unknown,
	retries: number,
	maxRetries: number,
) {
	const target = error as { details?: unknown };
	const details =
		typeof target.details === "object" && target.details !== null
			? (target.details as Record<string, unknown>)
			: {};
	try {
		target.details = {
			...details,
			transactionRetryCount: retries,
			maxTransactionRetries: maxRetries,
		};
	} catch {
		// Frozen error objects keep their original details.
	}
}

export class LixTransaction implements Reader {
	private finished = false;
	readonly #reader: ReaderContext = {
		receiver: "lixTransaction",
		run: (operation) => this.#run(operation),
		binding: () => this.binding,
		adopt: () => undefined,
	};

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

	/** Reads the transaction's state, including its own writes. */
	query<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: QueryOptions,
	): Promise<TRow[]> {
		return query<TRow>(this.#reader, sql, params, options);
	}

	queryOne<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: CallOptions,
	): Promise<TRow | undefined> {
		return queryOne<TRow>(this.#reader, sql, params, options);
	}

	/** Streams the transaction's state when it opens. The rows are buffered
	 * before the first page, under `maxBytes` (default 64 MiB). */
	stream<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: StreamOptions,
	): Promise<RowStream<TRow>> {
		return stream<TRow>(this.#reader, sql, params, options);
	}

	/** Runs any statement in this transaction. Writes become visible to others at `commit()`. */
	async execute<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options: ExecuteOptions = {},
	): Promise<StatementResult<TRow>> {
		assertSql("execute", "lixTransaction", sql);
		const native = nativeParams("execute", "lixTransaction", params);
		const bindingOptions = executeOptions("execute", "lixTransaction", options);
		throwIfAborted(options.signal);
		const statement: Partial<ExecuteResult> = wrapExecuteResult(
			await this.#run(() => this.binding.execute(sql, native, bindingOptions)),
		);
		// The durable receipt arrives from commit(). Deleting keeps a throwing
		// `rows` getter (duplicate columns) unevaluated.
		delete statement.commit;
		return statement as StatementResult<TRow>;
	}

	/** Stages one statement once per parameter set; all sets stage or none do. */
	async executeMany(
		sql: string,
		paramSets: readonly (readonly SqlParam[])[],
		options: ExecuteManyOptions = {},
	): Promise<{ rowsAffected: number }> {
		assertSql("executeMany", "lixTransaction", sql);
		const native = nativeParamSets("lixTransaction", paramSets);
		const bindingOptions = executeOptions("executeMany", "lixTransaction", options);
		throwIfAborted(options.signal);
		const { rowsAffected } = await this.#run(() =>
			this.binding.executeMany(sql, native, bindingOptions),
		);
		return { rowsAffected };
	}

	async #run<T>(operation: () => Promise<T>): Promise<T> {
		if (this.finished) throw transactionClosedError();
		return operation().catch((error: unknown) => {
			if ((error as { code?: string })?.code === "LIX_TRANSACTION_LOST") {
				this.finished = true;
				transactionFinalizer.unregister(this);
				this.onFinish();
			}
			throw error;
		});
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

function assertSql(operation: string, receiver: string, sql: unknown): void {
	if (typeof sql !== "string")
		throw invalidArgument(operation, "sql", "string", typeof sql, receiver);
}
