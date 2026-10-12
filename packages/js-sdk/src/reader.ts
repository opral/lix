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

// `await using` needs Symbol.asyncDispose. Engines without it get the
// registered symbol TypeScript's downlevel helpers also fall back to.
if (typeof (Symbol as { asyncDispose?: symbol }).asyncDispose !== "symbol") {
	Object.defineProperty(Symbol, "asyncDispose", {
		value: Symbol.for("Symbol.asyncDispose"),
	});
}

type LixError = Error & { code: string; hint?: string; details?: unknown };

function lixError(code: string, message: string, hint?: string): LixError {
	const error = new Error(message) as LixError;
	error.name = "LixError";
	error.code = code;
	if (hint !== undefined) error.hint = hint;
	return error;
}

export function snapshotClosedError(): LixError {
	return lixError("LIX_SNAPSHOT_CLOSED", "the snapshot is closed");
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
	return params.map((param, index) => toNativeValue(normalizeParam(param, index)));
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
	/** Tracks an open stream so the owner can close it. */
	adopt(stream: LixRowStream<object>): void;
};

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
	const result = await withAbort(
		context.run(() => context.binding().query(sql, native, bindingOptions)),
		options.signal,
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
	throwIfAborted(options.signal);
	const result = await withAbort(
		context.run(() => context.binding().query(sql, native, { single: true })),
		options.signal,
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
	const opening = context.run(() => context.binding().stream(sql, native, bindingOptions));
	let binding: QueryStreamBinding;
	try {
		binding = await withAbort(opening, options.signal);
	} catch (error) {
		// An abort that wins the race must still release the stream.
		void opening.then(
			(opened) => opened.cancel(),
			() => undefined,
		);
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

// An abandoned, unexhausted stream still pins its read state; release it.
const streamFinalizer = new FinalizationRegistry<QueryStreamBinding>(
	(binding) => void Promise.resolve(binding.cancel()).catch(() => undefined),
);

/**
 * Pulls are serialized: a pull issued while another is pending waits for it,
 * so rows always arrive in order. Closing settles a pending pull at once and
 * releases the engine stream. An error is latched: later pulls reject with it.
 */
export class LixRowStream<TRow extends object> implements RowStream<TRow> {
	readonly columns: string[];
	#tail: Promise<unknown> = Promise.resolve();
	#done = false;
	#failure: { error: unknown } | undefined;
	#closePromise: Promise<void> | undefined;
	#onClose = new Set<() => void>();
	#ended!: () => void;
	readonly #endedSignal = new Promise<typeof ENDED>((resolve) => {
		this.#ended = () => resolve(ENDED);
	});
	readonly #abort = () => void this.#closeWith({ error: this.signal?.reason });

	constructor(
		private readonly binding: QueryStreamBinding,
		private readonly signal?: AbortSignal,
	) {
		this.columns = Object.freeze([...binding.columns]) as string[];
		streamFinalizer.register(this, binding, this);
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
		let buffered: TRow[] = [];
		let index = 0;
		return {
			async next(): Promise<IteratorResult<TRow>> {
				while (index >= buffered.length) {
					const page = await stream.#nextPage();
					if (page === undefined) return { done: true, value: undefined };
					buffered = page;
					index = 0;
				}
				return { done: false, value: buffered[index++]! };
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

	[Symbol.asyncDispose](): Promise<void> {
		return this.close();
	}

	/** @internal Ends the stream with `error`: pending and later pulls reject with it. */
	fail(error: unknown): Promise<void> {
		return this.#closeWith({ error });
	}

	#nextPage(): Promise<TRow[] | undefined> {
		const pull = this.#tail.then(() => this.#pull());
		this.#tail = pull.catch(() => undefined);
		return pull;
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
			await this.#closeWith({ error });
			throw error;
		}
	}

	#end(): undefined {
		if (this.#failure) throw this.#failure.error;
		return undefined;
	}

	#closeWith(failure: { error: unknown } | undefined): Promise<void> {
		if (!this.#done && failure) this.#failure = failure;
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
				"LIX_AMBIGUOUS_COLUMN",
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
	readonly #streams = new Set<LixRowStream<object>>();
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
				this.#streams.add(stream);
				stream.onClose(() => this.#streams.delete(stream));
			},
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

	/** Idempotent. Closes its streams; later calls reject `LIX_SNAPSHOT_CLOSED`. */
	close(): Promise<void> {
		return (this.#closePromise ??= (async () => {
			this.#closed = true;
			snapshotFinalizer.unregister(this);
			const streams = [...this.#streams].map((stream) =>
				stream.fail(snapshotClosedError()),
			);
			this.#streams.clear();
			try {
				await Promise.allSettled(streams);
				await this.binding.close();
			} finally {
				this.onClose();
			}
		})());
	}

	[Symbol.asyncDispose](): Promise<void> {
		return this.close();
	}
}

export type { Reader };
