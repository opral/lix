/// <reference lib="esnext.disposable" preserve="true" />
import { invalidArgument } from "./errors.js";
import type {
	BindingExecuteOptions,
	BindingParam,
	BindingReadOptions,
	QueryStreamBinding,
	ReadSnapshotBinding,
	ReaderBinding,
} from "./binding-types.js";
import { rowsOf } from "./result.js";
import { normalizeParam, toNativeValue } from "./value.js";
import type {
	CallOptions,
	ExecuteOptions,
	ExecuteManyOptions,
	QueryOptions,
	Reader,
	Row,
	RowStream,
	Snapshot,
	SqlParam,
	StreamOptions,
} from "./types.js";

/**
 * The key of `[Symbol.asyncDispose]()`, for `await using`. Engines without
 * the symbol use the registered one; a `Symbol.asyncDispose` polyfill must
 * load before this module. Globals are never patched.
 */
const asyncDispose: typeof Symbol.asyncDispose =
	(Symbol as { asyncDispose?: typeof Symbol.asyncDispose }).asyncDispose ??
	(Symbol.for("Symbol.asyncDispose") as typeof Symbol.asyncDispose);

type LixError = Error & { code: string; hint?: string; details?: unknown };

function lixError(code: string, message: string, hint?: string): LixError {
	const error = new Error(message) as LixError;
	error.name = "LixError";
	error.code = code;
	if (hint !== undefined) error.hint = hint;
	return error;
}

export function snapshotClosedError(): LixError {
	return lixError("LIX_ERROR_SNAPSHOT_CLOSED", "the snapshot is closed");
}

export function closedError(): LixError {
	return lixError("LIX_ERROR_CLOSED", "Lix is closed");
}

/** Rejects with the signal's reason once it aborts, without stopping `pending`. */
export function withAbort<T>(pending: Promise<T>, signal?: AbortSignal): Promise<T> {
	if (!signal) return pending;
	if (signal.aborted) {
		void pending.catch(() => undefined);
		return Promise.reject(signal.reason);
	}
	return new Promise<T>((resolve, reject) => {
		const abort = () => {
			void pending.catch(() => undefined);
			reject(signal.reason);
		};
		signal.addEventListener("abort", abort, { once: true });
		pending.then(
			(value) => {
				signal.removeEventListener("abort", abort);
				resolve(value);
			},
			(error: unknown) => {
				signal.removeEventListener("abort", abort);
				reject(error);
			},
		);
	});
}

export function throwIfAborted(signal?: AbortSignal): void {
	if (signal?.aborted) throw signal.reason;
}

function assertSql(operation: string, receiver: string, sql: unknown): void {
	if (typeof sql !== "string")
		throw invalidArgument(operation, "sql", "string", typeof sql, receiver);
}

export function nativeParams(
	operation: string,
	receiver: string,
	params: readonly SqlParam[] | undefined,
): BindingParam[] {
	if (params === undefined) return [];
	if (!Array.isArray(params))
		throw invalidArgument(operation, "params", "array", typeof params, receiver);
	try {
		// `Array.from` visits holes, so a sparse array rejects instead of
		// binding nothing for the missing positions.
		return Array.from(params, (param, index) => toNativeValue(normalizeParam(param, index)));
	} catch (error) {
		const details = (error as { details?: { operation?: string } }).details;
		if (details && typeof details === "object") details.operation = operation;
		throw error;
	}
}

function assertOptions(
	operation: string,
	receiver: string,
	options: unknown,
): asserts options is Record<string, unknown> {
	if (!options || typeof options !== "object" || Array.isArray(options))
		throw invalidArgument(operation, "options", "object", typeof options, receiver);
	const signal = (options as { signal?: unknown }).signal;
	if (signal !== undefined && !(signal instanceof AbortSignal))
		throw invalidArgument(operation, "options.signal", "AbortSignal", typeof signal, receiver);
}

/** `maxBytes`: a positive safe integer, or `Infinity` for no cap. */
function assertMaxBytes(operation: string, receiver: string, maxBytes: unknown): void {
	if (maxBytes === undefined || maxBytes === Number.POSITIVE_INFINITY) return;
	if (typeof maxBytes !== "number" || !Number.isSafeInteger(maxBytes) || maxBytes < 1)
		throw invalidArgument(
			operation,
			"options.maxBytes",
			"positive integer or Infinity",
			String(maxBytes),
			receiver,
		);
}

export function readOptions(
	operation: string,
	receiver: string,
	options: QueryOptions | StreamOptions,
): BindingReadOptions {
	assertOptions(operation, receiver, options);
	assertMaxBytes(operation, receiver, options.maxBytes);
	return options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes };
}

export function executeOptions(
	operation: string,
	receiver: string,
	options: ExecuteOptions | ExecuteManyOptions,
): BindingExecuteOptions {
	assertOptions(operation, receiver, options);
	const { maxAutoCommitRetries, originKey, idempotencyKey } = options;
	if (
		maxAutoCommitRetries !== undefined &&
		(!Number.isInteger(maxAutoCommitRetries) ||
			maxAutoCommitRetries < 0 ||
			maxAutoCommitRetries > 0xffff_ffff)
	)
		throw invalidArgument(
			operation,
			"options.maxAutoCommitRetries",
			"integer between 0 and 4294967295",
			typeof maxAutoCommitRetries,
			receiver,
		);
	for (const [name, value] of [
		["originKey", originKey],
		["idempotencyKey", idempotencyKey],
	] as const) {
		if (value !== undefined && typeof value !== "string")
			throw invalidArgument(operation, `options.${name}`, "string", typeof value, receiver);
	}
	const maxBytes = (options as ExecuteOptions).maxBytes;
	assertMaxBytes(operation, receiver, maxBytes);
	return {
		...(originKey === undefined ? {} : { originKey }),
		...(maxAutoCommitRetries === undefined ? {} : { maxAutoCommitRetries }),
		...(idempotencyKey === undefined ? {} : { idempotencyKey }),
		...(maxBytes === undefined ? {} : { maxBytes }),
	};
}

export function nativeParamSets(
	receiver: string,
	paramSets: readonly (readonly SqlParam[])[],
): BindingParam[][] {
	if (!Array.isArray(paramSets))
		throw invalidArgument("executeMany", "paramSets", "array", typeof paramSets, receiver);
	return paramSets.map((params, setIndex) => {
		if (!Array.isArray(params))
			throw invalidArgument(
				"executeMany",
				`paramSets[${setIndex}]`,
				"array",
				typeof params,
				receiver,
			);
		try {
			return nativeParams("executeMany", receiver, params);
		} catch (error) {
			const target = error as { details?: unknown };
			if (target && typeof target === "object")
				target.details = {
					...(typeof target.details === "object" && target.details !== null
						? target.details
						: {}),
					paramSetIndex: setIndex,
				};
			throw error;
		}
	});
}

/** The runtime pieces `Reader` methods need from their owner. */
export type ReaderContext = {
	readonly receiver: string;
	/** Runs one binding call inside the owner's lifecycle gate. */
	run<T>(operation: () => Promise<T>): Promise<T>;
	/** The binding reads go to, resolved per call. */
	binding(): ReaderBinding;
	/**
	 * Tracks an open stream so the owner can close it. Throws (after
	 * releasing the stream) when the owner closed while the stream opened.
	 */
	adopt(stream: LixRowStream<object>): void;
	/** Aborts when the owner closes (or its transaction is aborted). */
	signal?(): AbortSignal | undefined;
};

/** A signal that aborts when any of `signals` aborts, with its reason. */
export function anySignal(
	signals: readonly (AbortSignal | undefined)[],
): AbortSignal | undefined {
	const present = signals.filter((signal): signal is AbortSignal => !!signal);
	if (present.length <= 1) return present[0];
	if (typeof AbortSignal.any === "function") return AbortSignal.any(present);
	const controller = new AbortController();
	for (const signal of present) {
		if (signal.aborted) {
			controller.abort(signal.reason);
			break;
		}
		signal.addEventListener("abort", () => controller.abort(signal.reason), {
			once: true,
		});
	}
	return controller.signal;
}

/** The signal a read passes to the engine: the caller's and the owner's. */
function readSignal(
	context: ReaderContext,
	signal: AbortSignal | undefined,
): AbortSignal | undefined {
	return anySignal([signal, context.signal?.()]);
}

export async function query<TRow extends object>(
	context: ReaderContext,
	sql: string,
	params: readonly SqlParam[] | undefined,
	options: QueryOptions = {},
): Promise<TRow[]> {
	assertSql("query", context.receiver, sql);
	const native = nativeParams("query", context.receiver, params);
	const bindingOptions = readOptions("query", context.receiver, options);
	throwIfAborted(options.signal);
	// The caller's signal and the owner's (closing the snapshot or Lix, or
	// aborting the transaction): either cancels the read with its reason.
	const signal = readSignal(context, options.signal);
	const result = await withAbort(
		context.run(() => context.binding().query(sql, native, bindingOptions, signal)),
		signal,
	);
	return rowsOf(result) as TRow[];
}

export async function queryOne<TRow extends object>(
	context: ReaderContext,
	sql: string,
	params: readonly SqlParam[] | undefined,
	options: CallOptions = {},
): Promise<TRow | undefined> {
	assertSql("queryOne", context.receiver, sql);
	const native = nativeParams("queryOne", context.receiver, params);
	assertOptions("queryOne", context.receiver, options);
	// At most two rows are read, so no byte cap applies.
	const bindingOptions = { single: true };
	throwIfAborted(options.signal);
	// The caller's signal and the owner's (closing the snapshot or Lix, or
	// aborting the transaction): either cancels the read with its reason.
	const signal = readSignal(context, options.signal);
	const result = await withAbort(
		context.run(() => context.binding().query(sql, native, bindingOptions, signal)),
		signal,
	);
	return rowsOf(result)[0] as TRow | undefined;
}

export async function stream<TRow extends object>(
	context: ReaderContext,
	sql: string,
	params: readonly SqlParam[] | undefined,
	options: StreamOptions = {},
): Promise<RowStream<TRow>> {
	assertSql("stream", context.receiver, sql);
	const native = nativeParams("stream", context.receiver, params);
	const bindingOptions = readOptions("stream", context.receiver, options);
	throwIfAborted(options.signal);
	const signal = readSignal(context, options.signal);
	const opening = context.run(() =>
		context
			.binding()
			.stream(sql, native, bindingOptions, signal),
	);
	let binding: QueryStreamBinding;
	try {
		binding = await withAbort(opening, signal);
	} catch (error) {
		// An abort that wins the race must still release the stream.
		void opening.then((opened) => opened.cancel()).catch(() => undefined);
		throw error;
	}
	try {
		assertUniqueColumns(binding.columns);
	} catch (error) {
		await Promise.resolve(binding.cancel()).catch(() => undefined);
		throw error;
	}
	const rows = new LixRowStream<TRow>(binding, options.signal);
	context.adopt(rows as LixRowStream<object>);
	return rows;
}

/**
 * The open streams or snapshots of an owner, held weakly: an owner closes
 * what is still open when it closes, but never keeps an abandoned one alive,
 * so the abandoned one's finalizer can release its engine state.
 */
export class WeakCollection<T extends object> {
	readonly #refs = new Set<WeakRef<T>>();
	readonly #registry = new FinalizationRegistry<WeakRef<T>>((ref) =>
		this.#refs.delete(ref),
	);

	/** Adds `item`; the returned function removes it. */
	add(item: T): () => void {
		const ref = new WeakRef(item);
		this.#refs.add(ref);
		this.#registry.register(item, ref, ref);
		return () => {
			this.#refs.delete(ref);
			this.#registry.unregister(ref);
		};
	}

	/** Removes and returns every item still alive. */
	drain(): T[] {
		const items: T[] = [];
		for (const ref of this.#refs) {
			this.#registry.unregister(ref);
			const item = ref.deref();
			if (item) items.push(item);
		}
		this.#refs.clear();
		return items;
	}

	get size(): number {
		return this.#refs.size;
	}
}

// An abandoned, unexhausted stream still pins its read state; release it.
const streamFinalizer = new FinalizationRegistry<QueryStreamBinding>(
	(binding) => void Promise.resolve(binding.cancel()).catch(() => undefined),
);

/**
 * One cursor serves `pages()` and the row iterator: a page pull returns the
 * rest of a page the row iterator started, so mixing them never skips or
 * repeats rows. Pulls are serialized: a pull issued while another is pending
 * waits for it, so rows always arrive in order, also under concurrent
 * `next()` calls. Closing settles a pending pull at once and releases the
 * engine stream. An error is latched: later pulls reject with it.
 */
export class LixRowStream<TRow extends object> implements RowStream<TRow> {
	readonly columns: readonly string[];
	#tail: Promise<unknown> = Promise.resolve();
	/** The page the row iterator is consuming, and its next row's index. */
	#rows: TRow[] = [];
	#index = 0;
	#done = false;
	#failure: { error: unknown } | undefined;
	#closePromise: Promise<void> | undefined;
	#onClose = new Set<() => void>();
	#ended!: () => void;
	readonly #endedSignal = new Promise<typeof ENDED>((resolve) => {
		this.#ended = () => resolve(ENDED);
	});
	/** Holds the stream weakly, so a long-lived signal never pins it. */
	readonly #abort: () => void;

	constructor(
		private readonly binding: QueryStreamBinding,
		private readonly signal?: AbortSignal,
	) {
		this.columns = Object.freeze([...binding.columns]);
		streamFinalizer.register(this, binding, this);
		const stream = new WeakRef(this);
		this.#abort = () => {
			const alive = stream.deref();
			if (alive)
				void alive.#closeWith({ error: signal?.reason }).catch(() => undefined);
		};
		if (signal?.aborted) this.#abort();
		else signal?.addEventListener("abort", this.#abort, { once: true });
	}

	/** @internal Runs `listener` once when the stream ends or closes. */
	onClose(listener: () => void): void {
		if (this.#done) listener();
		else this.#onClose.add(listener);
	}

	pages(): AsyncIterableIterator<TRow[]> {
		const stream = this;
		return {
			[Symbol.asyncIterator]() {
				return this;
			},
			async next(): Promise<IteratorResult<TRow[]>> {
				const page = await stream.#nextPage();
				return page === undefined
					? { done: true, value: undefined }
					: { done: false, value: page };
			},
			async return(): Promise<IteratorResult<TRow[]>> {
				await stream.close();
				return { done: true, value: undefined };
			},
			async throw(error?: unknown): Promise<IteratorResult<TRow[]>> {
				await stream.close();
				throw error;
			},
		};
	}

	[Symbol.asyncIterator](): AsyncIterator<TRow> {
		const stream = this;
		return {
			async next(): Promise<IteratorResult<TRow>> {
				const row = await stream.#nextRow();
				return row === undefined
					? { done: true, value: undefined }
					: { done: false, value: row };
			},
			async return(): Promise<IteratorResult<TRow>> {
				await stream.close();
				return { done: true, value: undefined };
			},
			async throw(error?: unknown): Promise<IteratorResult<TRow>> {
				await stream.close();
				throw error;
			},
		};
	}

	close(): Promise<void> {
		return this.#closeWith(undefined);
	}

	[asyncDispose](): Promise<void> {
		return this.close();
	}

	/** @internal Ends the stream with `error`: pending and later pulls reject with it. */
	fail(error: unknown): Promise<void> {
		return this.#closeWith({ error });
	}

	#nextPage(): Promise<TRow[] | undefined> {
		return this.#serialized(() => {
			if (this.#index < this.#rows.length) {
				const rest = this.#rows.slice(this.#index);
				this.#rows = [];
				this.#index = 0;
				return rest;
			}
			return this.#pull();
		});
	}

	#nextRow(): Promise<TRow | undefined> {
		return this.#serialized(async () => {
			while (this.#index >= this.#rows.length) {
				const page = await this.#pull();
				if (page === undefined) return undefined;
				this.#rows = page;
				this.#index = 0;
			}
			return this.#rows[this.#index++];
		});
	}

	#serialized<T>(pull: () => T | Promise<T>): Promise<T> {
		const next = this.#tail.then(pull);
		this.#tail = next.catch(() => undefined);
		return next;
	}

	async #pull(): Promise<TRow[] | undefined> {
		if (this.#done) return this.#end();
		try {
			const page = await Promise.race([this.binding.next(), this.#endedSignal]);
			if (this.#done || page === ENDED) return this.#end();
			if (page == null) {
				this.#finish();
				return undefined;
			}
			return rowsOf({ ...page, columns: this.columns }) as TRow[];
		} catch (error) {
			if (this.#done) return this.#end();
			// The read's own error wins over a failure to release the stream.
			await this.#closeWith({ error }).catch(() => undefined);
			throw error;
		}
	}

	#end(): undefined {
		if (this.#failure) throw this.#failure.error;
		return undefined;
	}

	#closeWith(failure: { error: unknown } | undefined): Promise<void> {
		if (!this.#done && failure) this.#failure = failure;
		this.#rows = [];
		this.#index = 0;
		this.#finish();
		return (this.#closePromise ??= Promise.resolve()
			.then(() => this.binding.cancel())
			.then(() => undefined));
	}

	#finish(): void {
		if (this.#done) return;
		this.#done = true;
		this.#ended();
		streamFinalizer.unregister(this);
		this.signal?.removeEventListener("abort", this.#abort);
		for (const listener of this.#onClose) listener();
		this.#onClose.clear();
	}
}

const ENDED = Symbol("stream ended");

function assertUniqueColumns(columns: readonly string[]): void {
	const seen = new Set<string>();
	for (const column of columns) {
		if (seen.has(column)) {
			throw lixError(
				"LIX_ERROR_AMBIGUOUS_COLUMN",
				`the result has more than one column named '${column}'`,
				"Alias the columns so every name is unique, e.g. SELECT a.id AS a_id, b.id AS b_id.",
			);
		}
		seen.add(column);
	}
}

// A snapshot that is never closed still pins its state; release it.
const snapshotFinalizer = new FinalizationRegistry<ReadSnapshotBinding>(
	(binding) => void binding.close().catch(() => undefined),
);

/** A pinned state: every read through it sees the same state. */
export class LixSnapshot implements Snapshot {
	#closePromise: Promise<void> | undefined;
	#closed = false;
	readonly #closing = new AbortController();
	readonly #streams = new WeakCollection<LixRowStream<object>>();
	readonly #context: ReaderContext;

	constructor(
		private readonly binding: ReadSnapshotBinding,
		private readonly onClose: () => void = () => undefined,
	) {
		snapshotFinalizer.register(this, binding, this);
		this.#context = {
			receiver: "snapshot",
			run: (operation) =>
				this.#closed ? Promise.reject(snapshotClosedError()) : operation(),
			binding: () => this.binding,
			adopt: (stream) => {
				if (this.#closed) {
					void stream.fail(snapshotClosedError()).catch(() => undefined);
					throw snapshotClosedError();
				}
				stream.onClose(this.#streams.add(stream));
			},
			signal: () => this.#closing.signal,
		};
	}

	query<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: QueryOptions,
	): Promise<TRow[]> {
		return query<TRow>(this.#context, sql, params, options);
	}

	queryOne<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: CallOptions,
	): Promise<TRow | undefined> {
		return queryOne<TRow>(this.#context, sql, params, options);
	}

	stream<TRow extends object = Row>(
		sql: string,
		params?: readonly SqlParam[],
		options?: StreamOptions,
	): Promise<RowStream<TRow>> {
		return stream<TRow>(this.#context, sql, params, options);
	}

	/** Idempotent. Closes its streams; later calls reject `LIX_ERROR_SNAPSHOT_CLOSED`. */
	close(): Promise<void> {
		return (this.#closePromise ??= (async () => {
			this.#closed = true;
			// Stop reads in flight in the engine, so closing never waits on them.
			this.#closing.abort(snapshotClosedError());
			snapshotFinalizer.unregister(this);
			const streams = this.#streams
				.drain()
				.map((stream) => stream.fail(snapshotClosedError()));
			try {
				await Promise.allSettled(streams);
				await this.binding.close();
			} finally {
				this.onClose();
			}
		})());
	}

	[asyncDispose](): Promise<void> {
		return this.close();
	}
}

export type { Reader };
