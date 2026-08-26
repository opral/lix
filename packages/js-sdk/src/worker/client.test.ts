import { expect, test, vi } from "vitest";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";
import { BindingLease, LixWorkerClient, workerBinding } from "./client.js";
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

test("browser sync resolves fresh headers for reconnect requests", async () => {
	const transport = fakeConnection();
	let generation = 0;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/repository",
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
		url: "https://example.test/repository",
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
			url: "https://example.test/repository/lix/v1/sync/pull",
			method: "GET",
			headers: [["authorization", "Bearer fresh"]],
			credentials: "include",
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

test("browser sync cancels a streamed response at the Rust byte limit", async () => {
	const transport = fakeConnection();
	let streamCancelled = false;
	let fetchAborted = false;
	const client = new LixWorkerClient(transport.connection);
	client.beginLease(undefined, undefined, {
		url: "https://example.test/repository",
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
			url: "https://example.test/repository/lix/v1/sync/pull",
			method: "GET",
			headers: [],
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
		url: "https://example.test/repository",
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
			url: "https://example.test/repository/lix/v1/sync/pull",
			method: "GET",
			headers: [],
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
