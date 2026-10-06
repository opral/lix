import { afterEach, expect, test, vi } from "vitest";

const host = vi.hoisted(() => ({ close: vi.fn(), connect: vi.fn() }));
vi.mock("./repository-host.js", () => ({ createRepositoryHost: () => host }));
afterEach(() => {
	vi.unstubAllGlobals();
	vi.useRealTimers();
	vi.resetModules();
	host.close.mockReset();
	host.connect.mockReset();
});
async function start() {
	vi.useFakeTimers();
	const post = vi.fn();
	const close = vi.fn();
	const channel = { onmessage: undefined as any, postMessage: vi.fn(), close: vi.fn() };
	vi.stubGlobal("onmessage", undefined);
	vi.stubGlobal("postMessage", post);
	vi.stubGlobal("close", close);
	vi.stubGlobal("location", { href: "https://example.test/owner.js" });
	vi.stubGlobal("BroadcastChannel", class { constructor() { return channel; } });
	vi.stubGlobal("MessageChannel", class {
		port1 = { postMessage: vi.fn(), addEventListener: vi.fn(), start: vi.fn(), close: vi.fn(), onmessage: undefined };
		port2 = {};
	});
	vi.stubGlobal("navigator", {
		locks: { request: (_name: string, options: any, callback?: () => Promise<void>) =>
			Promise.resolve((callback ?? options)()) },
	});
	await import("./entry.repository.browser.js");
	(globalThis as any).onmessage({ data: { kind: "start", key: "repo", channelName: "channel", token: "run" } });
	return { post, close, channel };
}

test("cleanup timeout reports failure before the worker closes", async () => {
	host.close.mockReturnValue(new Promise(() => {}));
	const worker = await start();
	(globalThis as any).onmessage({ data: { kind: "release" } });
	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.post).toHaveBeenCalledWith({ kind: "failure", token: "run", message: "Repository owner cleanup timed out" });
	expect(worker.post.mock.invocationCallOrder.at(-1)).toBeLessThan(worker.close.mock.invocationCallOrder[0]);
	expect(worker.close).toHaveBeenCalledOnce();
});

test("dead-client cleanup timeout reports failure and owner loss before closing", async () => {
	const worker = await start();
	worker.channel.onmessage({ data: { kind: "discover", client: "client", nonce: "probe" } });
	const owner = worker.channel.postMessage.mock.calls.find(([message]) => message.kind === "owner")![0];
	worker.channel.onmessage({ data: { kind: "connect", client: "client", generation: owner.generation, lease: "client-lease" } });
	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.post).toHaveBeenCalledWith({ kind: "failure", token: "run", message: "Repository client cleanup timed out" });
	expect(worker.channel.postMessage).toHaveBeenCalledWith({ kind: "gone", generation: owner.generation });
	expect(worker.post.mock.invocationCallOrder.at(-1)).toBeLessThan(worker.close.mock.invocationCallOrder[0]);
	expect(worker.close).toHaveBeenCalledOnce();
});
