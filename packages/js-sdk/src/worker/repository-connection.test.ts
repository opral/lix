import { afterEach, expect, test, vi } from "vitest";
import { createRepositoryConnection } from "./factory.browser.js";

const connections: Array<ReturnType<typeof createRepositoryConnection>> = [];
afterEach(async () => {
	for (const c of connections.splice(0)) await c.terminate().catch(() => {});
	vi.unstubAllGlobals();
	vi.useRealTimers();
});
function connection() {
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
	const result = createRepositoryConnection(crypto.randomUUID());
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
