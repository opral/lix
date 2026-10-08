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

async function start(holdClientLease = false) {
	vi.useFakeTimers();
	const post = vi.fn();
	const close = vi.fn();
	let emit: ((message: any) => void) | undefined;
	const receive = vi.fn();
	const disconnect = vi.fn(() => new Promise<void>(() => {}));
	host.connect.mockImplementation((output) => {
		emit = output;
		return { receive, disconnect };
	});
	const channel = {
		onmessage: undefined as any,
		postMessage: vi.fn(),
		close: vi.fn(),
	};
	vi.stubGlobal("onmessage", undefined);
	vi.stubGlobal("postMessage", post);
	vi.stubGlobal("close", close);
	vi.stubGlobal("location", { href: "https://example.test/owner.js" });
	vi.stubGlobal("BroadcastChannel", class {
		constructor() {
			return channel;
		}
	});
	vi.stubGlobal("MessageChannel", class {
		port1 = {
			postMessage: vi.fn(),
			addEventListener: vi.fn(),
			start: vi.fn(),
			close: vi.fn(),
			onmessage: undefined,
		};
		port2 = {};
	});
	vi.stubGlobal("navigator", {
		locks: {
			request: (
				_name: string,
				options: any,
				callback?: () => Promise<void>,
			) =>
				holdClientLease && _name === "client-lease"
					? new Promise(() => {})
					: Promise.resolve((callback ?? options)()),
		},
	});
	await import("./entry.repository.browser.js");
	(globalThis as any).onmessage({
		data: {
			kind: "start",
			key: "repo",
			channelName: "channel",
			token: "run",
		},
	});
	return {
		post,
		close,
		channel,
		receive,
		disconnect,
		emit: (message: any) => emit!(message),
	};
}

type TestWorker = Awaited<ReturnType<typeof start>>;

function connect(worker: TestWorker, client = "client") {
	worker.channel.onmessage({
		data: { kind: "discover", client, nonce: "probe" },
	});
	const owner = worker.channel.postMessage.mock.calls.find(
		([message]) => message.kind === "owner",
	)![0];
	worker.channel.onmessage({
		data: {
			kind: "connect",
			client,
			generation: owner.generation,
			lease: "client-lease",
		},
	});
	return owner.generation as string;
}

test("cleanup timeout reports failure before the worker closes", async () => {
	host.close.mockReturnValue(new Promise(() => {}));
	const worker = await start();
	(globalThis as any).onmessage({ data: { kind: "release" } });
	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.post).toHaveBeenCalledWith({
		kind: "failure",
		token: "run",
		message: "Repository owner cleanup timed out",
	});
	expect(worker.post.mock.invocationCallOrder.at(-1)).toBeLessThan(
		worker.close.mock.invocationCallOrder[0],
	);
	expect(worker.close).toHaveBeenCalledOnce();
});

test("dead-client cleanup timeout reports failure and owner loss before closing", async () => {
	const worker = await start();
	const generation = connect(worker);
	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.post).toHaveBeenCalledWith({
		kind: "failure",
		token: "run",
		message: "Repository client cleanup timed out",
	});
	expect(worker.channel.postMessage).toHaveBeenCalledWith({
		kind: "gone",
		generation,
	});
	expect(worker.post.mock.invocationCallOrder.at(-1)).toBeLessThan(
		worker.close.mock.invocationCallOrder[0],
	);
	expect(worker.close).toHaveBeenCalledOnce();
});

test("same-worker client routing forwards input and completed responses directly", async () => {
	const worker = await start(true);
	vi.stubGlobal("MessageChannel", class {
		constructor() {
			throw new Error("same-worker routing must not add a message task");
		}
	});
	const generation = connect(worker);
	const request = {
		id: 1,
		sessionId: 0,
		operation: { kind: "execute", sql: "SELECT 1", params: [] },
	};
	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation, message: request },
	});
	expect(worker.receive).toHaveBeenCalledWith(request);
	const response = { id: 1, ok: true, value: { rows: [] } };
	worker.emit(response);
	expect(worker.channel.postMessage).toHaveBeenLastCalledWith({
		kind: "output",
		client: "client",
		generation,
		message: response,
	});
});

test("disconnect completion clears the client timeout and retires its route", async () => {
	const worker = await start();
	worker.disconnect.mockImplementation(async () => {
		worker.emit({ kind: "repository.disconnected" });
	});
	const generation = connect(worker);
	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.disconnect).toHaveBeenCalledOnce();
	expect(worker.post).not.toHaveBeenCalledWith(
		expect.objectContaining({ kind: "failure" }),
	);
	const request = {
		id: 2,
		sessionId: 0,
		operation: { kind: "execute", sql: "SELECT 1", params: [] },
	};
	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation, message: request },
	});
	expect(worker.receive).not.toHaveBeenCalled();
});

test("a rejected disconnect keeps the client death watchdog armed", async () => {
	const worker = await start();
	worker.disconnect.mockRejectedValue(new Error("disconnect send failed"));
	const generation = connect(worker);
	await vi.advanceTimersByTimeAsync(4999);
	expect(worker.post).not.toHaveBeenCalledWith(
		expect.objectContaining({ kind: "failure" }),
	);
	expect(worker.close).not.toHaveBeenCalled();

	await vi.advanceTimersByTimeAsync(1);
	expect(worker.post).toHaveBeenCalledWith({
		kind: "failure",
		token: "run",
		message: "Repository client cleanup timed out",
	});
	expect(worker.channel.postMessage).toHaveBeenCalledWith({
		kind: "gone",
		generation,
	});
	expect(worker.close).toHaveBeenCalledOnce();
});

test("only an admitted teardown callback routes while disconnect is pending", async () => {
	const worker = await start();
	let finishDisconnect!: () => void;
	worker.disconnect.mockImplementation(
		() =>
			new Promise<void>((resolve) => {
				finishDisconnect = resolve;
			}),
	);
	const generation = connect(worker);
	expect(worker.disconnect).toHaveBeenCalledOnce();

	const callback = { kind: "sync.headers", requestId: 21 };
	worker.emit(callback);
	expect(worker.channel.postMessage).toHaveBeenLastCalledWith({
		kind: "output",
		client: "client",
		generation,
		message: callback,
	});

	const result = {
		kind: "sync.headers.result",
		requestId: 21,
		result: { ok: true, headers: [["authorization", "admitted-token"]] },
	};
	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation: "foreign", message: result },
	});
	worker.channel.onmessage({
		data: {
			kind: "input",
			client: "client",
			generation,
			message: { ...result, requestId: 99 },
		},
	});
	expect(worker.receive).not.toHaveBeenCalled();

	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation, message: result },
	});
	expect(worker.receive).toHaveBeenCalledOnce();
	expect(worker.receive).toHaveBeenCalledWith(result);
	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation, message: result },
	});
	expect(worker.receive).toHaveBeenCalledOnce();

	worker.emit({ kind: "repository.disconnected" });
	finishDisconnect();
	const messageCountAfterRetirement = worker.channel.postMessage.mock.calls.length;
	worker.emit({ kind: "sync.headers", requestId: 22 });
	worker.channel.onmessage({
		data: { kind: "input", client: "client", generation, message: result },
	});
	expect(worker.channel.postMessage).toHaveBeenCalledTimes(
		messageCountAfterRetirement,
	);
	expect(worker.receive).toHaveBeenCalledOnce();

	await vi.advanceTimersByTimeAsync(5000);
	expect(worker.post).not.toHaveBeenCalledWith(
		expect.objectContaining({ kind: "failure" }),
	);
});
