import type {
	BindingExecuteResult,
	BindingBatchStatement,
	BindingObserveEvent,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "../binding-types.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteOptions,
	LixBatchOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	RemoteLixServerOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "../types.js";
import type { NativeLixValue } from "../value.js";
import {
	decodeExecuteResult,
	decodeHandshake,
	decodeObserveEvent,
	encodeWireValue,
	errorFromResponseBody,
	protocolError,
	record,
	REMOTE_PROTOCOL_PATH,
	remoteError,
	type RemoteObserveSubscription,
} from "./protocol.js";
import { readSseEvents } from "./sse.js";

const OBSERVE_RETRY_BASE_MS = 100;
const OBSERVE_RETRY_MAX_MS = 5_000;
const REMOTE_SESSION_HEADER = "Lix-Session-Id";
const MIN_COMPRESSIBLE_JSON_BYTES = 32 * 1024;
const COMPRESSION_SAMPLE_BYTES = 32 * 1024;
const MAX_COMPRESSION_SAMPLE_RATIO = 0.7;
const MAX_COMPRESSED_BODY_RATIO = 0.9;

export async function openRemoteLixBinding(
	options: RemoteLixServerOptions,
): Promise<LixBinding> {
	const client = new RemoteLixBinding(options);
	await client.open();
	return client;
}

class RemoteLixBinding implements LixBinding {
	readonly #baseUrl: URL;
	readonly #fetch: NonNullable<RemoteLixServerOptions["fetch"]>;
	readonly #headers: RemoteLixServerOptions["headers"];
	readonly #observationHub: RemoteObservationHub;
	#sessionId: string | undefined;
	#acceptingOperations = true;
	#operationQueue: Promise<void> = Promise.resolve();
	#closePromise: Promise<void> | undefined;

	constructor(options: RemoteLixServerOptions) {
		if (!options || typeof options !== "object") {
			throw new TypeError("openLix() remote server must be an object");
		}
		if (options.mode !== "remote") {
			throw new TypeError("openLix() remote server mode must be 'remote'");
		}
		this.#baseUrl = protocolBaseUrl(options.url);
		const remoteFetch = options.fetch ?? globalThis.fetch?.bind(globalThis);
		if (typeof remoteFetch !== "function") {
			throw new TypeError("openLix() remote mode requires fetch");
		}
		this.#fetch = remoteFetch;
		if (
			options.headers !== undefined &&
			typeof options.headers !== "function" &&
			!isHeadersInit(options.headers)
		) {
			throw new TypeError(
				"openLix() remote server headers must be HeadersInit or a function",
			);
		}
		this.#headers = options.headers;
		this.#observationHub = new RemoteObservationHub({
			openStream: (subscriptions, signal) =>
				this.#requestObserveStream(subscriptions, signal),
		});
	}

	async open(): Promise<void> {
		const handshake = decodeHandshake(
			await this.#requestJson("", { method: "GET" }),
		);
		this.#sessionId = handshake.sessionId;
	}

	async execute(
		sql: string,
		params: NativeLixValue[],
		options?: ExecuteOptions,
	): Promise<BindingExecuteResult> {
		this.#assertOpen();
		return this.#enqueue(async () =>
			decodeExecuteResult(
				await this.#requestJson("execute", {
					method: "POST",
					body: JSON.stringify({
						sql,
						params: params.map(encodeWireValue),
						...(options === undefined ? {} : { options }),
					}),
				}),
			),
		);
	}

	async executeBatch(
		statements: BindingBatchStatement[],
		options?: LixBatchOptions,
	): Promise<BindingExecuteResult[]> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const value = await this.#requestJson("execute-batch", {
				method: "POST",
				body: JSON.stringify({
					statements: statements.map((statement) => ({
						sql: statement.sql,
						params: statement.params.map(encodeWireValue),
					})),
					...(options === undefined ? {} : { options }),
				}),
			});
			if (!Array.isArray(value)) {
				throw protocolError("execute batch response must be an array");
			}
			return value.map(decodeExecuteResult);
		});
	}

	async observe(
		sql: string,
		params: NativeLixValue[],
	): Promise<ObserveEventsBinding> {
		this.#assertOpen();
		return this.#enqueue(async () =>
			this.#observationHub.observe(sql, params.map(encodeWireValue)),
		);
	}

	async beginTransaction(): Promise<LixTransactionBinding> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("beginTransaction");
	}

	async activeBranchId(): Promise<string> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const handshake = decodeHandshake(
				await this.#requestJson("", { method: "GET" }),
			);
			if (handshake.sessionId !== this.#sessionId) {
				throw protocolError("remote handshake changed sessionId");
			}
			return handshake.activeBranchId;
		});
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const value = record(
				await this.#requestJson("branch/create", {
					method: "POST",
					body: JSON.stringify(options),
				}),
				"create branch response",
			);
			if (
				typeof value.id !== "string" ||
				typeof value.name !== "string" ||
				typeof value.hidden !== "boolean" ||
				typeof value.commitId !== "string"
			) {
				throw protocolError("create branch response is invalid");
			}
			return {
				id: value.id,
				name: value.name,
				hidden: value.hidden,
				commitId: value.commitId,
			};
		});
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		this.#assertOpen();
		return this.#enqueue(async () => {
			const value = record(
				await this.#requestJson("branch/switch", {
					method: "POST",
					body: JSON.stringify(options),
				}),
				"switch branch response",
			);
			if (value.branchId !== options.branchId) {
				throw protocolError("switch branch response is invalid");
			}
			return { branchId: options.branchId };
		});
	}

	async importFilesystemPaths(_paths: string[]): Promise<void> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("importFilesystemPaths");
	}

	async mergeBranchPreview(
		_options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("mergeBranchPreview");
	}

	async mergeBranch(
		_options: MergeBranchOptions,
	): Promise<MergeBranchReceipt> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("mergeBranch");
	}

	async syncDiskToLix(): Promise<void> {
		this.#assertOpen();
		throw unsupportedRemoteOperation("syncDiskToLix");
	}

	async close(): Promise<void> {
		if (this.#closePromise !== undefined) return this.#closePromise;
		this.#acceptingOperations = false;
		this.#observationHub.close();
		this.#closePromise = this.#enqueue(async () => {
			this.#observationHub.close();
			await this.#requestJson("session", { method: "DELETE" }, "empty");
		});
		return this.#closePromise;
	}

	async #requestJson(
		path: string,
		init: RequestInit,
		responseKind: "json" | "empty" = "json",
	): Promise<unknown> {
		const headers = new Headers(await resolveHeaders(this.#headers));
		if (this.#sessionId === undefined) headers.delete(REMOTE_SESSION_HEADER);
		else headers.set(REMOTE_SESSION_HEADER, this.#sessionId);
		headers.set("accept", "application/json");
		headers.delete("content-encoding");
		let requestInit = init;
		if (init.body !== undefined) {
			headers.set("content-type", "application/json");
			if (
				typeof init.body === "string" &&
				init.body.length >= MIN_COMPRESSIBLE_JSON_BYTES
			) {
				const prepared = await prepareJsonRequestBody(init.body);
				requestInit = { ...init, body: prepared.body };
				if (prepared.compressed) headers.set("content-encoding", "gzip");
			}
		}
		let response: Response;
		try {
			response = await this.#fetch(new URL(path, this.#baseUrl), {
				...requestInit,
				headers,
			});
		} catch (cause) {
			throw remoteError(
				"LIX_REMOTE_UNAVAILABLE",
				"The remote Lix server is unavailable",
				{ details: { cause: errorMessage(cause) } },
			);
		}
		if (!response.ok) throw await errorFromHttpResponse(response);
		if (responseKind === "empty" || response.status === 204) return undefined;
		const text = await response.text();
		try {
			return JSON.parse(text);
		} catch {
			throw protocolError(
				`remote response ${response.status} did not contain valid JSON`,
			);
		}
	}

	async #requestObserveStream(
		subscriptions: RemoteObserveSubscription[],
		signal: AbortSignal,
	): Promise<Response> {
		let headers: Headers;
		try {
			headers = new Headers(await resolveHeaders(this.#headers));
		} catch (cause) {
			throw remoteError(
				"LIX_REMOTE_CONFIGURATION_ERROR",
				"Remote Lix observation headers could not be resolved",
				{ details: { cause: errorMessage(cause) } },
			);
		}
		if (this.#sessionId === undefined) {
			throw protocolError("remote observation started without a session");
		}
		headers.set(REMOTE_SESSION_HEADER, this.#sessionId);
		headers.set("accept", "text/event-stream");
		headers.set("content-type", "application/json");
		headers.delete("content-encoding");
		const observeBody = JSON.stringify({ subscriptions });
		const prepared =
			observeBody.length < MIN_COMPRESSIBLE_JSON_BYTES
				? { body: observeBody, compressed: false }
				: await prepareJsonRequestBody(observeBody);
		if (prepared.compressed) {
			headers.set("content-encoding", "gzip");
		}
		try {
			return await this.#fetch(new URL("observe/multiplex", this.#baseUrl), {
				method: "POST",
				headers,
				body: prepared.body,
				signal,
			});
		} catch (cause) {
			if (signal.aborted) throw cause;
			throw remoteError(
				"LIX_REMOTE_UNAVAILABLE",
				"The remote Lix observation stream is unavailable",
				{ details: { cause: errorMessage(cause) } },
			);
		}
	}

	#assertOpen(): void {
		if (!this.#acceptingOperations) {
			throw remoteError("LIX_ERROR_CLOSED", "Lix is closed");
		}
	}

	#enqueue<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.#operationQueue.then(operation, operation);
		this.#operationQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}
}

type ObserveWaiter = {
	resolve(value: BindingObserveEvent | undefined): void;
	reject(error: unknown): void;
};

type ObserveOutcome =
	| { ok: true; event: BindingObserveEvent }
	| { ok: false; error: unknown };

type RemoteObservationHubOptions = {
	openStream(
		subscriptions: RemoteObserveSubscription[],
		signal: AbortSignal,
	): Promise<Response>;
};

class RemoteObservationHub {
	readonly #openStream: RemoteObservationHubOptions["openStream"];
	readonly #observations = new Map<string, RemoteObservation>();
	#nextObservationId = 0;
	#controller: AbortController | undefined;
	#retryTimer: ReturnType<typeof setTimeout> | undefined;
	#retryAttempt = 0;
	#serverRetryMs: number | undefined;
	#generation = 0;
	#closed = false;

	constructor(options: RemoteObservationHubOptions) {
		this.#openStream = options.openStream;
	}

	observe(
		sql: string,
		params: RemoteObserveSubscription["params"],
	): RemoteObservation {
		const id = `observe-${++this.#nextObservationId}`;
		const observation = new RemoteObservation({
			id,
			sql,
			params,
			onClose: () => {
				this.#observations.delete(id);
				this.#restartStream();
			},
		});
		this.#observations.set(id, observation);
		this.#restartStream();
		return observation;
	}

	close(): void {
		if (!this.#closed) {
			this.#closed = true;
			this.#stopStream();
		}
		for (const observation of [...this.#observations.values()]) {
			observation.close();
		}
		this.#observations.clear();
	}

	#restartStream(): void {
		this.#stopStream();
		this.#startStream();
	}

	#startStream(): void {
		if (
			this.#closed ||
			this.#observations.size === 0 ||
			this.#controller !== undefined ||
			this.#retryTimer !== undefined
		) {
			return;
		}
		const generation = this.#generation;
		const controller = new AbortController();
		this.#controller = controller;
		void this.#consume(generation, controller);
	}

	#stopStream(): void {
		this.#generation += 1;
		this.#controller?.abort();
		this.#controller = undefined;
		if (this.#retryTimer !== undefined) clearTimeout(this.#retryTimer);
		this.#retryTimer = undefined;
		this.#retryAttempt = 0;
		this.#serverRetryMs = undefined;
	}

	async #consume(
		generation: number,
		controller: AbortController,
	): Promise<void> {
		let reconnect = false;
		let streamOpened = false;
		try {
			const response = await this.#openStream(
				[...this.#observations.values()].map((observation) =>
					observation.request(),
				),
				controller.signal,
			);
			if (!this.#isCurrent(generation, controller)) return;
			streamOpened = true;
			if (!response.ok) {
				if (isRetryableObserveStatus(response.status)) {
					void response.body?.cancel();
					reconnect = true;
					return;
				}
				const error = await errorFromHttpResponse(response);
				if (this.#isCurrent(generation, controller)) {
					this.#failStream(error, controller);
				}
				return;
			}
			if (!response.body) {
				this.#failStream(
					protocolError("remote observe response has no body"),
					controller,
				);
				return;
			}
			const contentType = response.headers.get("content-type") ?? "";
			if (
				contentType.split(";", 1)[0]?.trim().toLowerCase() !==
				"text/event-stream"
			) {
				this.#failStream(
					protocolError("remote observe response must be text/event-stream"),
					controller,
				);
				return;
			}
			for await (const frame of readSseEvents(response.body)) {
				if (!this.#isCurrent(generation, controller)) return;
				if (frame.retry !== undefined) this.#serverRetryMs = frame.retry;
				if (frame.event === "next") {
					try {
						const payload = record(
							JSON.parse(frame.data),
							"remote multiplex observe next event",
						);
						const observation = this.#observation(payload.subscriptionId);
						observation.accept(decodeObserveEvent(payload));
						this.#retryAttempt = 0;
					} catch (error) {
						this.#failStream(
							asObserveProtocolError(error, "next"),
							controller,
						);
						return;
					}
				} else if (frame.event === "error") {
					try {
						const payload = record(
							JSON.parse(frame.data),
							"remote multiplex observe error event",
						);
						if (
							payload.retryable !== undefined &&
							typeof payload.retryable !== "boolean"
						) {
							throw protocolError(
								"remote observe error retryable must be a boolean",
							);
						}
						const error = errorFromResponseBody(payload);
						if (payload.subscriptionId !== undefined) {
							const observation = this.#observation(payload.subscriptionId);
							if (payload.retryable === true) {
								observation.recover(error);
								reconnect = true;
								controller.abort();
								return;
							}
							observation.fail(error);
							continue;
						}
						if (payload.retryable === true) {
							for (const observation of this.#observations.values()) {
								observation.recover(error);
							}
							reconnect = true;
							controller.abort();
						} else {
							this.#failStream(error, controller);
						}
					} catch (error) {
						this.#failStream(
							asObserveProtocolError(error, "error"),
							controller,
						);
					}
					return;
				} else if (frame.event !== "message" || frame.data.length > 0) {
					this.#failStream(
						protocolError(`unknown remote observe event: ${frame.event}`),
						controller,
					);
					return;
				}
			}
			if (this.#isCurrent(generation, controller)) reconnect = true;
		} catch (error) {
			if (
				!this.#isCurrent(generation, controller) ||
				controller.signal.aborted
			) {
				return;
			}
			if (streamOpened || isRetryableObserveError(error)) reconnect = true;
			else this.#failStream(error, controller);
		} finally {
			if (this.#isCurrent(generation, controller)) {
				this.#controller = undefined;
				if (reconnect) this.#scheduleReconnect(generation);
			}
		}
	}

	#observation(id: unknown): RemoteObservation {
		if (typeof id !== "string" || id.length === 0) {
			throw protocolError("remote observe event requires subscriptionId");
		}
		const observation = this.#observations.get(id);
		if (!observation) {
			throw protocolError(`unknown remote observe subscription: ${id}`);
		}
		return observation;
	}

	#failAll(error: unknown): void {
		for (const observation of this.#observations.values()) {
			observation.fail(error);
		}
	}

	#failStream(error: unknown, controller: AbortController): void {
		this.#failAll(error);
		controller.abort();
	}

	#scheduleReconnect(generation: number): void {
		if (
			this.#closed ||
			this.#observations.size === 0 ||
			generation !== this.#generation ||
			this.#controller !== undefined ||
			this.#retryTimer !== undefined
		) {
			return;
		}
		const delay =
			this.#serverRetryMs === undefined
				? Math.min(
						OBSERVE_RETRY_BASE_MS * 2 ** this.#retryAttempt,
						OBSERVE_RETRY_MAX_MS,
					)
				: Math.min(
						Math.max(this.#serverRetryMs, OBSERVE_RETRY_BASE_MS),
						OBSERVE_RETRY_MAX_MS,
					);
		this.#retryAttempt += 1;
		this.#retryTimer = setTimeout(() => {
			this.#retryTimer = undefined;
			if (generation === this.#generation) this.#startStream();
		}, delay);
	}

	#isCurrent(generation: number, controller: AbortController): boolean {
		return (
			!this.#closed &&
			generation === this.#generation &&
			controller === this.#controller
		);
	}
}

type RemoteObservationOptions = RemoteObserveSubscription & {
	onClose(): void;
};

class RemoteObservation implements ObserveEventsBinding {
	readonly #id: string;
	readonly #sql: string;
	readonly #params: RemoteObserveSubscription["params"];
	readonly #onClose: () => void;
	#outcomes: ObserveOutcome[] = [];
	#waiters: ObserveWaiter[] = [];
	#terminalError: unknown;
	#lastRows: BindingExecuteResult | undefined;
	#lastSequence = -1;
	#closed = false;

	constructor(options: RemoteObservationOptions) {
		this.#id = options.id;
		this.#sql = options.sql;
		this.#params = options.params;
		this.#onClose = options.onClose;
	}

	request(): RemoteObserveSubscription {
		return { id: this.#id, sql: this.#sql, params: this.#params };
	}

	next(): Promise<BindingObserveEvent | undefined> {
		const outcome = this.#outcomes.shift();
		if (outcome?.ok) return Promise.resolve(outcome.event);
		if (outcome) return Promise.reject(outcome.error);
		if (this.#terminalError !== undefined) {
			return Promise.reject(this.#terminalError);
		}
		if (this.#closed) return Promise.resolve(undefined);
		return new Promise((resolve, reject) => {
			this.#waiters.push({ resolve, reject });
		});
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#outcomes = [];
		this.#terminalError = undefined;
		for (const waiter of this.#waiters.splice(0)) waiter.resolve(undefined);
		this.#onClose();
	}

	accept(event: BindingObserveEvent): void {
		if (this.#closed || this.#terminalError !== undefined) return;
		if (
			this.#lastRows !== undefined &&
			executeResultsEqual(this.#lastRows, event.rows)
		) {
			return;
		}
		const normalized = {
			sequence: this.#lastSequence + 1,
			mutationSequence: event.mutationSequence,
			rows: event.rows,
		};
		this.#lastRows = event.rows;
		this.#lastSequence = normalized.sequence;
		const waiter = this.#waiters.shift();
		if (waiter) waiter.resolve(normalized);
		else {
			this.#outcomes = this.#outcomes.filter((outcome) => !outcome.ok);
			this.#outcomes.push({ ok: true, event: normalized });
		}
	}

	recover(error: unknown): void {
		if (this.#closed || this.#terminalError !== undefined) return;
		const waiter = this.#waiters.shift();
		if (waiter) waiter.reject(error);
		else if (!this.#outcomes.some((outcome) => !outcome.ok)) {
			this.#outcomes.push({ ok: false, error });
		}
	}

	fail(error: unknown): void {
		if (this.#closed || this.#terminalError !== undefined) return;
		this.#terminalError = error;
		for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
	}
}

async function prepareJsonRequestBody(
	body: string,
): Promise<{ body: BodyInit; compressed: boolean }> {
	const bytes = new TextEncoder().encode(body);
	if (bytes.byteLength < MIN_COMPRESSIBLE_JSON_BYTES) {
		return { body, compressed: false };
	}
	const sample = bytes.subarray(
		0,
		Math.min(bytes.byteLength, COMPRESSION_SAMPLE_BYTES),
	);
	const compressedSample = await gzipBytes(sample);
	if (
		compressedSample.byteLength >
		sample.byteLength * MAX_COMPRESSION_SAMPLE_RATIO
	) {
		return { body, compressed: false };
	}
	const compressed = await gzipBytes(bytes);
	if (compressed.byteLength > bytes.byteLength * MAX_COMPRESSED_BODY_RATIO) {
		return { body, compressed: false };
	}
	const transportBody = new ArrayBuffer(compressed.byteLength);
	new Uint8Array(transportBody).set(compressed);
	return { body: transportBody, compressed: true };
}

async function gzipBytes(bytes: Uint8Array): Promise<Uint8Array> {
	const CompressionStreamConstructor = (
		globalThis as typeof globalThis & {
			CompressionStream?: new (
				format: "gzip",
			) => TransformStream<Uint8Array, Uint8Array>;
		}
	).CompressionStream;
	if (typeof CompressionStreamConstructor === "function") {
		const stream = new CompressionStreamConstructor("gzip");
		const output = new Response(stream.readable).arrayBuffer();
		const writer = stream.writable.getWriter();
		await writer.write(bytes);
		await writer.close();
		return new Uint8Array(await output);
	}
	const { gzipSync } = await import("fflate");
	return gzipSync(bytes, { level: 1 });
}

async function resolveHeaders(
	headers: RemoteLixServerOptions["headers"],
): Promise<HeadersInit | undefined> {
	return typeof headers === "function" ? await headers() : headers;
}

async function errorFromHttpResponse(response: Response): Promise<Error> {
	const text = await response.text();
	try {
		return errorFromResponseBody(JSON.parse(text), response.status);
	} catch (error) {
		if (
			error instanceof Error &&
			"status" in error &&
			(error as { status?: number }).status === response.status
		) {
			return error;
		}
		return remoteError(
			"LIX_REMOTE_REQUEST_FAILED",
			`Remote Lix request failed with status ${response.status}`,
			{
				status: response.status,
				details: text.length === 0 ? undefined : { body: text.slice(0, 1000) },
			},
		);
	}
}

function isRetryableObserveStatus(status: number): boolean {
	return status === 408 || status === 429 || status >= 500;
}

function isRetryableObserveError(error: unknown): boolean {
	return (
		error instanceof Error &&
		"code" in error &&
		(error as { code?: string }).code === "LIX_REMOTE_UNAVAILABLE"
	);
}

function asObserveProtocolError(error: unknown, event: string): Error {
	if (
		error instanceof Error &&
		"code" in error &&
		(error as { code?: string }).code === "LIX_REMOTE_PROTOCOL_ERROR"
	) {
		return error;
	}
	return protocolError(
		`remote observe ${event} event contains invalid data: ${errorMessage(error)}`,
	);
}

function executeResultsEqual(
	left: BindingExecuteResult,
	right: BindingExecuteResult,
): boolean {
	return (
		left.rowsAffected === right.rowsAffected &&
		stringArraysEqual(left.columns, right.columns) &&
		left.rows.length === right.rows.length &&
		left.rows.every(
			(row, rowIndex) =>
				row.length === right.rows[rowIndex]?.length &&
				row.every((value, valueIndex) =>
					nativeValuesEqual(value, right.rows[rowIndex]?.[valueIndex]),
				),
		) &&
		left.notices.length === right.notices.length &&
		left.notices.every((notice, index) => {
			const other = right.notices[index];
			return (
				notice.code === other?.code &&
				notice.message === other.message &&
				notice.hint === other.hint
			);
		})
	);
}

function nativeValuesEqual(
	left: NativeLixValue,
	right: NativeLixValue | undefined,
): boolean {
	if (!right || left.kind !== right.kind) return false;
	switch (left.kind) {
		case "blob":
			return (
				right.kind === "blob" &&
				left.blob.length === right.blob.length &&
				left.blob.every((byte, index) => byte === right.blob[index])
			);
		case "json":
			return right.kind === "json" && jsonValuesEqual(left.value, right.value);
		default:
			return left.value === right.value;
	}
}

function jsonValuesEqual(left: unknown, right: unknown): boolean {
	if (left === right) return true;
	if (Array.isArray(left) || Array.isArray(right)) {
		return (
			Array.isArray(left) &&
			Array.isArray(right) &&
			left.length === right.length &&
			left.every((value, index) => jsonValuesEqual(value, right[index]))
		);
	}
	if (!left || !right || typeof left !== "object" || typeof right !== "object") {
		return false;
	}
	const leftRecord = left as Record<string, unknown>;
	const rightRecord = right as Record<string, unknown>;
	const leftKeys = Object.keys(leftRecord).sort();
	const rightKeys = Object.keys(rightRecord).sort();
	return (
		stringArraysEqual(leftKeys, rightKeys) &&
		leftKeys.every((key) => jsonValuesEqual(leftRecord[key], rightRecord[key]))
	);
}

function stringArraysEqual(left: string[], right: string[]): boolean {
	return (
		left.length === right.length &&
		left.every((value, index) => value === right[index])
	);
}

function protocolBaseUrl(value: string | URL): URL {
	let workspaceUrl: URL;
	try {
		workspaceUrl = new URL(value);
	} catch {
		throw new TypeError("openLix() remote server url must be an absolute URL");
	}
	if (workspaceUrl.protocol !== "http:" && workspaceUrl.protocol !== "https:") {
		throw new TypeError("openLix() remote server url must use http or https");
	}
	if (workspaceUrl.search || workspaceUrl.hash) {
		throw new TypeError(
			"openLix() remote server url must not contain a query or fragment",
		);
	}
	workspaceUrl.pathname = `${workspaceUrl.pathname.replace(/\/$/, "")}${REMOTE_PROTOCOL_PATH}`;
	return workspaceUrl;
}

function unsupportedRemoteOperation(operation: string): Error & { code: string } {
	return remoteError(
		"LIX_UNSUPPORTED_REMOTE_OPERATION",
		`${operation} is not supported in remote mode`,
		{ details: { operation } },
	);
}

function isHeadersInit(value: unknown): value is HeadersInit {
	try {
		new Headers(value as HeadersInit);
		return true;
	} catch {
		return false;
	}
}

function errorMessage(value: unknown): string {
	return value instanceof Error ? value.message : String(value);
}
