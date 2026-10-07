import { afterEach, expect, test, vi } from "vitest";
import { createRepositoryConnection } from "./factory.browser.js";

const currentBuild = "https://example.test/assets/current-worker.js";
const connections: Array<ReturnType<typeof createRepositoryConnection>> = [];
afterEach(async () => {
	for (const c of connections.splice(0)) await c.terminate().catch(() => {});
	vi.unstubAllGlobals();
	vi.useRealTimers();
});
function connection(key = crypto.randomUUID()) {
	const sent: any[] = [];
	let channel: any;
	const worker = {
		postMessage: vi.fn(),
		terminate: vi.fn(),
		addEventListener: vi.fn(),
		removeEventListener: vi.fn(),
	};
	vi.stubGlobal(
		"Worker",
		class {
			constructor() {
				return worker;
			}
		},
	);
	vi.stubGlobal(
		"BroadcastChannel",
		class {
			onmessage: any;
			close = vi.fn();
			postMessage = (message: any) => sent.push(message);
			constructor() {
				channel = this;
			}
		},
	);
	vi.stubGlobal("navigator", {
		locks: {
			request: async (_name: string, callback: () => Promise<void>) =>
				callback(),
		},
	});
	worker.postMessage.mockImplementation((message: any) => {
		if (message.kind === "start") {
			const listener = worker.addEventListener.mock.calls.find(([kind]) => kind === "message")?.[1];
			listener?.({ data: { kind: "build", buildId: currentBuild, token: message.token } });
		}
	});
	const result = createRepositoryConnection(key);
	connections.push(result);
	const listener = vi.fn(),
		fatal = vi.fn();
	result.onMessage(listener);
	result.onFatal(fatal);
	const receive = (message: any) => channel.onmessage({ data: message });
	const discover = () =>
		sent.findLast((message) => message.kind === "discover");
	const elect = (generation = "owner-1") => {
		const query = discover();
		receive({
			kind: "owner",
		client: query.client,
		nonce: query.nonce,
		generation,
		buildId: currentBuild,
		});
		receive({ kind: "connected", client: query.client, generation });
		return query.client;
	};
	return {
		result,
		worker,
		sent,
		channel,
		receive,
		elect,
		listener,
		fatal,
		discover,
	};
}
test("queues initial open until elected owner connects and rejects stale output", async () => {
	const c = connection();
	c.result.postMessage({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: { kind: "memory" },
			telemetryEnabled: false,
			progressEnabled: false,
		},
	});
	expect(c.sent.some((m) => m.kind === "input")).toBe(false);
	const client = c.elect();
	await Promise.resolve();
	expect(c.sent.filter((m) => m.kind === "input")).toHaveLength(1);
	c.receive({
		kind: "output",
		client,
		generation: "old",
		message: { id: 1, ok: true },
	});
	expect(c.listener).not.toHaveBeenCalled();
	c.receive({
		kind: "output",
		client,
		generation: "owner-1",
		message: {
			id: 1,
			ok: true,
			context: { branchId: "main", accountId: "account" },
		},
	});
	expect(c.listener).toHaveBeenCalledOnce();
	const closing = c.result.terminate();
	c.receive({ kind: "disconnected", client, generation: "owner-1" });
	await closing;
	expect(c.worker.postMessage).toHaveBeenCalledWith({ kind: "release" });
});

test("routes the final session-close callback while termination awaits owner cleanup", async () => {
	const repositoryId = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
	const c = connection();
	const client = c.elect();
	const closing = c.result.terminate();
	c.receive({
		kind: "output",
		client,
		generation: "owner-1",
		message: {
			kind: "sync.fetch",
			requestId: 77,
			request: {
				url: `https://example.test/lix/v1/${repositoryId}/session`,
				method: "DELETE",
				headers: [["lix-session-id", "session-a"]],
				response: { mode: "buffered", maxBytes: 128 },
			},
		},
	});
	const request = c.listener.mock.calls
		.map(([message]) => message)
		.find((message) => message.kind === "sync.fetch");
	expect(request).toBeDefined();
	const callbackId = request.requestId;
	c.result.postMessage({
		kind: "sync.fetch.result",
		requestId: callbackId,
		result: { ok: true, response: { status: 204, statusText: "No Content", headers: [], body: new Uint8Array() } },
	});
	expect(c.sent).toContainEqual({
		kind: "input",
		client,
		generation: "owner-1",
		message: {
			kind: "sync.fetch.result",
			requestId: 77,
			result: { ok: true, response: { status: 204, statusText: "No Content", headers: [], body: new Uint8Array() } },
		},
	});
	c.result.postMessage({
		kind: "sync.fetch.result",
		requestId: 999,
		result: { ok: true, response: { status: 200, statusText: "OK", headers: [], body: new Uint8Array() } },
	});
	expect(c.sent.filter((message) => message.kind === "input")).toHaveLength(1);
	c.receive({ kind: "disconnected", client, generation: "owner-1" });
	await closing;
});
test("owner replacement reconnects without a fatal error", async () => {
	const c = connection();
	const client = c.elect();
	await Promise.resolve();
	c.elect("owner-2");
	await Promise.resolve();
	expect(c.fatal).not.toHaveBeenCalled();
	const closing = c.result.terminate();
	c.receive({ kind: "disconnected", client, generation: "owner-2" });
	await closing;
});
test("discovery and unacknowledged close are bounded", async () => {
	vi.useFakeTimers();
	const c = connection();
	await vi.advanceTimersByTimeAsync(30000);
	expect(c.fatal).toHaveBeenCalledWith(
		expect.objectContaining({ code: "LIX_OPEN_TIMEOUT" }),
	);
	await c.result.terminate();
	const d = connection();
	d.elect();
	const closing = d.result.terminate();
	const failed = expect(closing).rejects.toMatchObject({
		code: "LIX_OWNER_CLOSE_FAILED",
	});
	await vi.advanceTimersByTimeAsync(5000);
	await failed;
	expect(d.worker.postMessage).toHaveBeenCalledWith({ kind: "release" });
});
test("late discovery after a timeout cannot attach an abandoned client", async () => {
	vi.useFakeTimers();
	const c = connection();
	await vi.advanceTimersByTimeAsync(30000);
	const count = c.sent.filter((m) => m.kind === "connect").length;
	c.elect();
	expect(c.sent.filter((m) => m.kind === "connect")).toHaveLength(count);
	await c.result.terminate();
});

test("resuming a suspended page probes before declaring its owner lost", async () => {
	vi.useFakeTimers();
	const c = connection();
	const client = c.elect();
	await vi.advanceTimersByTimeAsync(250);
	vi.setSystemTime(Date.now() + 60000);
	await vi.advanceTimersByTimeAsync(250);
	const resumedProbe = c.discover();
	const failedOnResume = c.fatal.mock.calls.length;
	c.receive({
		kind: "owner",
		client,
		nonce: resumedProbe.nonce,
		buildId: currentBuild,
		generation: "owner-1",
	});
	const closing = c.result.terminate();
	c.receive({ kind: "disconnected", client, generation: "owner-1" });
	await closing;
	expect(failedOnResume).toBe(0);
});
test("a silent owner still times out after the page resumes", async () => {
	vi.useFakeTimers();
	const c = connection();
	c.elect();
	vi.setSystemTime(Date.now() + 60000);
	await vi.advanceTimersByTimeAsync(250);
	const failedOnResume = c.fatal.mock.calls.length;
	await vi.advanceTimersByTimeAsync(30250);
	expect(c.fatal).toHaveBeenCalledWith(
		expect.objectContaining({ code: "LIX_OWNER_LOST" }),
	);
	await c.result.terminate();
	expect(failedOnResume).toBe(0);
});

for (const buildId of [undefined, "https://example.test/assets/old-worker.js"]) {
	test(
		`rejects ${buildId ? "different-build" : "legacy"} owners before sending operations`,
		async () => {
			const c = connection();
			const query = c.discover();
			c.receive({
				kind: "owner",
				client: query.client,
				nonce: query.nonce,
				generation: "old",
				buildId,
			});
			expect(c.fatal).toHaveBeenCalledWith(
				expect.objectContaining({ code: "LIX_OWNER_VERSION_MISMATCH" }),
			);
			expect(c.sent.some((m) => m.kind === "connect" || m.kind === "input")).toBe(false);
			expect(c.worker.terminate).not.toHaveBeenCalled();
			await c.result.terminate();
		},
	);
}

test("owner acquisition failure is reported promptly and stale run failures are ignored", async () => {
	const c = connection();
	const token = c.worker.postMessage.mock.calls.find(([message]) => message.kind === "start")![0].token;
	const receive = c.worker.addEventListener.mock.calls.find(([kind]) => kind === "message")![1];
	receive({ data: { kind: "failure", token: "old-run", message: "stale" } });
	expect(c.fatal).not.toHaveBeenCalled();
	receive({ data: { kind: "failure", token, message: "ownership unavailable" } });
	expect(c.fatal).toHaveBeenCalledWith(expect.objectContaining({ code: "LIX_WORKER_FAILED", message: "ownership unavailable" }));
	expect(c.worker.terminate).toHaveBeenCalledOnce();
	await c.result.terminate();
});

test("an idle worker that errors is evicted before the next open", async () => {
	const c = connection();
	const client = c.elect();
	const token = c.worker.postMessage.mock.calls.find(([message]) => message.kind === "start")![0].token;
	const receive = c.worker.addEventListener.mock.calls.find(([kind]) => kind === "message")![1];
	const closing = c.result.terminate();
	c.receive({ kind: "disconnected", client, generation: "owner-1" });
	await closing;
	receive({ data: { kind: "retired", token, reusable: true, runtimeWarm: true } });
	const idleError = c.worker.addEventListener.mock.calls.findLast(([kind]) => kind === "error")![1];
	idleError(new Error("idle worker failed"));
	expect(c.worker.terminate).toHaveBeenCalledOnce();
	const replacement = connection();
	expect(replacement.worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ kind: "start" }));
	await replacement.result.terminate();
});


test("same-key reopen racing retirement restarts the retained realm after cleanup", async () => {
	const key = crypto.randomUUID();
	const c = connection(key);
	const client = c.elect();
	const receive = c.worker.addEventListener.mock.calls.find(([kind]) => kind === "message")![1];
	const firstStart = c.worker.postMessage.mock.calls.find(([message]) => message.kind === "start")![0];
	const closing = c.result.terminate();
	c.receive({ kind: "disconnected", client, generation: "owner-1" });
	await closing;
	const reopened = createRepositoryConnection(key);
	connections.push(reopened);
	const fatal = vi.fn();
	reopened.onFatal(fatal);
	expect(c.worker.postMessage.mock.calls.filter(([message]) => message.kind === "start")).toHaveLength(1);
	receive({ data: { kind: "retired", token: firstStart.token, reusable: true, runtimeWarm: true } });
	const starts = c.worker.postMessage.mock.calls.filter(([message]) => message.kind === "start");
	expect(starts).toHaveLength(2);
	expect(starts[1][0].token).not.toBe(firstStart.token);
	expect(c.worker.terminate).not.toHaveBeenCalled();
	receive({ data: { kind: "failure", token: firstStart.token, message: "old retirement" } });
	expect(fatal).not.toHaveBeenCalled();
	const reopenedClient = c.elect("owner-2");
	const secondClose = reopened.terminate();
	c.receive({ kind: "disconnected", client: reopenedClient, generation: "owner-2" });
	await secondClose;
	receive({ data: { kind: "retired", token: starts[1][0].token, reusable: false } });
});
