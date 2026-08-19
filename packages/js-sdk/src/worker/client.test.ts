import { expect, test } from "vitest";
import type {
	WorkerConnection,
	WorkerInput,
	WorkerResponse,
} from "./protocol.js";
import { LixWorkerClient } from "./client.js";
import { responseFromSyncFetch } from "./host.js";

function fakeConnection() {
	const sent: WorkerInput[] = [];
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
		terminateImmediately() {},
		async terminate() {},
	};
	return { connection, sent, emit: (message: WorkerResponse) => onMessage(message) };
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
