import { expect, test, vi } from "vitest";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";
import { SNAPSHOT_RESTORE_CHUNK_BYTES } from "../snapshot-restore.js";
import {
	BindingLease,
	LixWorkerClient,
	pumpSnapshotToWorker,
	workerBinding,
} from "./client.js";
import { responseFromSyncFetch } from "./host.js";

function fakeConnection(options: { terminate?: () => Promise<void> } = {}) {
	const sent: WorkerInput[] = [];
	let terminateCount = 0;
	let onMessage: (message: WorkerResponse) => void = () => undefined;
	const connection: WorkerConnection = {
		postMessage(message) {
			sent.push(message);
		},
		onMessage(listener) {
			onMessage = listener;
		},
		onFatal() {},
		ref() {},
		unref() {},
		async terminate() {
			terminateCount += 1;
			await options.terminate?.();
		},
	};
	return {
		connection,
		sent,
		emit: (message: WorkerResponse) => onMessage(message),
		terminateCount: () => terminateCount,
	};
}

test("failed close terminates the worker before releasing its binding", async () => {
	const termination = deferred<void>();
	const events: string[] = [];
	const transport = fakeConnection({
		terminate: async () => {
			events.push("worker termination started");
			await termination.promise;
			events.push("worker termination finished");
		},
	});
	const client = new LixWorkerClient(transport.connection);
	client.beginLease();
	let released = false;
	const binding = workerBinding(
		client,
		new BindingLease(() => {
			released = true;
			events.push("binding released");
		}),
		0,
	);

	const closing = binding.close();
	const request = transport.sent.find(
		(message): message is Extract<WorkerInput, { id: number }> =>
			"id" in message && message.operation.kind === "close",
	);
	if (!request) throw new Error("expected a worker close request");
	transport.emit({
		id: request.id,
		ok: false,
		error: {
			name: "LixError",
			message: "sync drain failed",
			code: "LIX_ERROR_SYNC",
		},
	});

	await vi.waitFor(() => {
		expect(events).toEqual(["worker termination started"]);
	});
	expect(released).toBe(false);
	termination.resolve();
	await expect(closing).rejects.toThrow("sync drain failed");
	expect(transport.terminateCount()).toBe(1);
	expect(client.isDisposed).toBe(true);
	expect(released).toBe(true);
	expect(events).toEqual([
		"worker termination started",
		"worker termination finished",
		"binding released",
	]);
});

test("open progress crosses the worker boundary without controlling open", () => {
	const transport = fakeConnection();
	const progress = vi.fn(() => {
		throw new Error("consumer callback failed");
	});
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, undefined, progress);

	expect(() =>
		transport.emit({
			kind: "open.progress",
			progress: {
				phase: "migrating",
				fromFormat: 74,
				toFormat: 75,
				completed: 1,
				total: 2,
			},
		}),
	).not.toThrow();
	expect(progress).toHaveBeenCalledWith({
		phase: "migrating",
		fromFormat: 74,
		toFormat: 75,
		completed: 1,
		total: 2,
	});
});

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

test("worker restore posts bounded owned chunks with request backpressure", async () => {
	const transport = fakeConnection();
	const client = new LixWorkerClient(transport.connection);
	client.beginLease();
	const callerChunk = new Uint8Array(SNAPSHOT_RESTORE_CHUNK_BYTES * 2 + 11);
	for (let index = 0; index < callerChunk.byteLength; index++) {
		callerChunk[index] = index % 239;
	}
	const source = new ReadableStream<Uint8Array>(
		{
			start(controller) {
				controller.enqueue(callerChunk);
				controller.close();
			},
		},
		{ highWaterMark: 0 },
	);
	const open = deferred<unknown>();
	const pumping = pumpSnapshotToWorker(client, source.getReader(), 73, open.promise);
	const postedChunks: Uint8Array[] = [];

	for (let index = 0; index < 3; index++) {
		await vi.waitFor(() => {
			expect(
				transport.sent.filter(
					(message) =>
						"id" in message &&
						message.operation.kind === "openSnapshot.write",
				).length,
			).toBe(index + 1);
		});
		const request = transport.sent.at(-1);
		if (
			!request ||
			!("id" in request) ||
			request.operation.kind !== "openSnapshot.write"
		) {
			throw new Error("expected a snapshot write request");
		}
		postedChunks.push(request.operation.chunk);
		expect(request.operation.chunk.byteLength).toBeLessThanOrEqual(
			SNAPSHOT_RESTORE_CHUNK_BYTES,
		);
		expect(request.operation.chunk.buffer).not.toBe(callerChunk.buffer);
		transport.emit({ id: request.id, ok: true });
	}
	await vi.waitFor(() => {
		expect(
			transport.sent.some(
				(message) =>
					"id" in message &&
					message.operation.kind === "openSnapshot.finish",
			),
		).toBe(true);
	});
	const finish = transport.sent.at(-1);
	if (
		!finish ||
		!("id" in finish) ||
		finish.operation.kind !== "openSnapshot.finish"
	) {
		throw new Error("expected a snapshot finish request");
	}
	transport.emit({ id: finish.id, ok: true });
	open.resolve(undefined);
	await pumping;

	expect(postedChunks.map((chunk) => chunk.byteLength)).toEqual([
		SNAPSHOT_RESTORE_CHUNK_BYTES,
		SNAPSHOT_RESTORE_CHUNK_BYTES,
		11,
	]);
	const restored = new Uint8Array(callerChunk.byteLength);
	let offset = 0;
	for (const chunk of postedChunks) {
		restored.set(chunk, offset);
		offset += chunk.byteLength;
	}
	expect(restored).toEqual(callerChunk);
	await client.terminate();
});

test("worker open rejection interrupts a stalled source tail", async () => {
	const transport = fakeConnection();
	const client = new LixWorkerClient(transport.connection);
	client.beginLease();
	let sent = false;
	let canceled = false;
	const source = new ReadableStream<Uint8Array>(
		{
			pull(controller) {
				if (sent) return;
				sent = true;
				controller.enqueue(new TextEncoder().encode("not a snapshot"));
			},
			cancel() {
				canceled = true;
			},
		},
		{ highWaterMark: 0 },
	);
	const open = deferred<unknown>();
	const pumping = pumpSnapshotToWorker(
		client,
		source.getReader(),
		91,
		open.promise,
	);
	await vi.waitFor(() => {
		expect(
			transport.sent.some(
				(message) =>
					"id" in message &&
					message.operation.kind === "openSnapshot.write",
			),
		).toBe(true);
	});
	const write = transport.sent.at(-1);
	if (
		!write ||
		!("id" in write) ||
		write.operation.kind !== "openSnapshot.write"
	) {
		throw new Error("expected a snapshot write request");
	}
	transport.emit({ id: write.id, ok: true });
	const semanticError = Object.assign(new Error("invalid snapshot header"), {
		name: "LixError",
		code: "LIX_INVALID_SNAPSHOT",
	});
	open.reject(semanticError);

	await expect(pumping).rejects.toBe(semanticError);
	expect(canceled).toBe(true);
	expect(
		transport.sent.some(
			(message) =>
				"kind" in message &&
				message.kind === "openSnapshot.cancel" &&
				message.snapshotId === 91,
		),
	).toBe(true);
	await client.terminate();
});

test("browser sync resolves fresh headers for reconnect requests", async () => {
	const transport = fakeConnection();
	let generation = 0;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		headers: async () => ({ Authorization: `Bearer token-${++generation}` }),
	});

	transport.emit({ kind: "sync.headers", requestId: 41 });
	await Promise.resolve();
	await Promise.resolve();
	transport.emit({ kind: "sync.headers", requestId: 42 });
	await Promise.resolve();
	await Promise.resolve();

	expect(transport.sent).toContainEqual({
		kind: "sync.headers.result",
		requestId: 41,
		result: { ok: true, headers: [["authorization", "Bearer token-1"]] },
	});
	expect(transport.sent).toContainEqual({
		kind: "sync.headers.result",
		requestId: 42,
		result: { ok: true, headers: [["authorization", "Bearer token-2"]] },
	});
});

test("browser sync custom fetch crosses the worker boundary", async () => {
	const transport = fakeConnection();
	const seen: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async (input, init) => {
			seen.push({ input, init });
			return new Response("ok", {
				status: 200,
				headers: { "server-timing": "lix-server-protocol;dur=1" },
			});
		},
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 7,
		request: {
			url: "https://example.test/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/sync/pull",
			method: "GET",
			headers: [["authorization", "Bearer fresh"]],
			credentials: "include",
			responseMode: "buffered",
			responseLimit: 1024,
		},
	});
	await new Promise((resolve) => setTimeout(resolve, 0));

	expect(seen).toHaveLength(1);
	expect(new Headers(seen[0]?.init?.headers).get("authorization")).toBe(
		"Bearer fresh",
	);
	const result = transport.sent.find(
		(message) => "kind" in message && message.kind === "sync.fetch.result",
	);
	expect(result).toMatchObject({
		kind: "sync.fetch.result",
		requestId: 7,
		result: { ok: true, response: { status: 200 } },
	});
	if (
		!result ||
		!("kind" in result) ||
		result.kind !== "sync.fetch.result" ||
		!result.result.ok
	) {
		throw new Error("expected a successful sync fetch result");
	}
	expect(new TextDecoder().decode(result.result.response.body)).toBe("ok");
	expect(new Headers(result.result.response.headers).get("server-timing")).toBe(
		"lix-server-protocol;dur=1",
	);
});

test("browser authority streams cross the worker boundary with pull backpressure", async () => {
	const transport = fakeConnection();
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async () =>
			new Response(
				new ReadableStream<Uint8Array>({
					start(controller) {
						controller.enqueue(new Uint8Array([1, 2]));
						controller.enqueue(new Uint8Array([3]));
						controller.close();
					},
				}),
				{ status: 200, headers: { "content-type": "text/event-stream" } },
			),
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 10,
		request: {
			url: "https://example.test/lix/v1/repository/observe",
			method: "POST",
			headers: [],
			responseMode: "stream",
		},
	});
	await new Promise((resolve) => setTimeout(resolve, 0));
	expect(transport.sent).toContainEqual({
		kind: "sync.fetch.result",
		requestId: 10,
		result: {
			ok: true,
			response: {
				status: 200,
				statusText: "",
				headers: [["content-type", "text/event-stream"]],
				streaming: true,
			},
		},
	});

	for (const expected of [
		{ ok: true, done: false, chunk: new Uint8Array([1, 2]) },
		{ ok: true, done: false, chunk: new Uint8Array([3]) },
		{ ok: true, done: true },
	]) {
		transport.emit({ kind: "sync.fetch.stream.pull", requestId: 10 });
		await new Promise((resolve) => setTimeout(resolve, 0));
		const results = transport.sent.filter(
			(message) =>
				"kind" in message &&
				message.kind === "sync.fetch.stream.result" &&
				message.requestId === 10,
		);
		expect(results.at(-1)).toMatchObject({
			kind: "sync.fetch.stream.result",
			requestId: 10,
			result: expected,
		});
	}
});

test("browser sync cancels a streamed response at the Rust byte limit", async () => {
	const transport = fakeConnection();
	let streamCancelled = false;
	let fetchAborted = false;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async (_input, init) => {
			init?.signal?.addEventListener("abort", () => {
				fetchAborted = true;
			});
			return new Response(
				new ReadableStream<Uint8Array>({
					start(controller) {
						controller.enqueue(new Uint8Array([1, 2, 3]));
						controller.enqueue(new Uint8Array([4, 5, 6]));
					},
					cancel() {
						streamCancelled = true;
					},
				}),
			);
		},
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 9,
		request: {
			url: "https://example.test/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/sync/pull",
			method: "GET",
			headers: [],
			responseMode: "buffered",
			responseLimit: 4,
		},
	});
	await new Promise((resolve) => setTimeout(resolve, 0));

	expect(streamCancelled).toBe(true);
	expect(fetchAborted).toBe(true);
	const result = transport.sent.find(
		(message) =>
			"kind" in message &&
			message.kind === "sync.fetch.result" &&
			message.requestId === 9,
	);
	expect(result).toMatchObject({
		kind: "sync.fetch.result",
		requestId: 9,
		result: {
			ok: false,
			error: {
				code: "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE",
				message: "sync fetch response exceeds 4 bytes",
			},
		},
	});
});

test("browser sync cancellation aborts the bridged fetch", async () => {
	const transport = fakeConnection();
	let aborted = false;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async (_input, init) =>
			await new Promise<Response>((_resolve, reject) => {
				init?.signal?.addEventListener(
					"abort",
					() => {
						aborted = true;
						reject(init.signal?.reason);
					},
					{ once: true },
				);
			}),
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 8,
		request: {
			url: "https://example.test/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/sync/pull",
			method: "GET",
			headers: [],
			responseMode: "buffered",
			responseLimit: 1024,
		},
	});
	await Promise.resolve();
	transport.emit({ kind: "sync.fetch.cancel", requestId: 8 });
	await new Promise((resolve) => setTimeout(resolve, 0));

	expect(aborted).toBe(true);
	expect(
		transport.sent.some(
			(message) =>
				"kind" in message &&
				message.kind === "sync.fetch.result" &&
				message.requestId === 8,
		),
	).toBe(false);
});

test("browser sync ignores a fetch that resolves after cancellation", async () => {
	const transport = fakeConnection();
	const lateResponse = deferred<Response>();
	let streamCancelled = false;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async () => lateResponse.promise,
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 11,
		request: {
			url: "https://example.test/lix/v1/repository/observe",
			method: "POST",
			headers: [],
			responseMode: "stream",
		},
	});
	await Promise.resolve();
	transport.emit({ kind: "sync.fetch.cancel", requestId: 11 });
	lateResponse.resolve(
		new Response(
			new ReadableStream<Uint8Array>({
				cancel() {
					streamCancelled = true;
				},
			}),
		),
	);
	await new Promise((resolve) => setTimeout(resolve, 0));

	expect(streamCancelled).toBe(true);
	expect(
		transport.sent.some(
			(message) =>
				"kind" in message &&
				message.kind === "sync.fetch.result" &&
				message.requestId === 11,
		),
	).toBe(false);
});

test("browser sync does not revive a bodyless stream after cancellation", async () => {
	const transport = fakeConnection();
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: async () => new Response(null, { status: 204 }),
	});

	transport.emit({
		kind: "sync.fetch",
		requestId: 12,
		request: {
			url: "https://example.test/lix/v1/repository/observe",
			method: "POST",
			headers: [],
			responseMode: "stream",
		},
	});
	await new Promise((resolve) => setTimeout(resolve, 0));
	transport.emit({ kind: "sync.fetch.cancel", requestId: 12 });
	transport.emit({ kind: "sync.fetch.stream.pull", requestId: 12 });
	await new Promise((resolve) => setTimeout(resolve, 0));

	expect(
		transport.sent.some(
			(message) =>
				"kind" in message &&
				message.kind === "sync.fetch.stream.result" &&
				message.requestId === 12,
		),
	).toBe(false);
});

test.each([204, 205, 304])(
	"browser sync reconstructs bodyless %s responses",
	(status) => {
		const response = responseFromSyncFetch({
			status,
			statusText: "",
			headers: [],
			body: new Uint8Array(),
		});
		expect(response.status).toBe(status);
		expect(response.body).toBeNull();
	},
);
