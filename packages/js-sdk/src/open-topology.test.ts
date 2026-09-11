import { beforeEach, expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import type { LixStorage } from "./storage-adapter.js";
import { openLix } from "./open-lix.js";
const mocks = vi.hoisted(() => ({ local: vi.fn(), remote: vi.fn() }));
vi.mock("./worker/client.js", () => ({ openLixWorkerBinding: mocks.local }));
vi.mock("./remote/client.js", () => ({ openRemoteLixBinding: mocks.remote }));
beforeEach(() => {
	mocks.local
		.mockReset()
		.mockResolvedValue({ close: async () => undefined } as LixBinding);
	mocks.remote
		.mockReset()
		.mockResolvedValue({ close: async () => undefined } as LixBinding);
});
const storage = () =>
	({
		lixStorage: {
			version: 1,
			config: { kind: "filesystem", path: "/fixture" },
			connect: vi.fn(),
		},
	}) as unknown as LixStorage;
const server = {
	url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
};

test("no options opens memory; storage alone opens local", async () => {
	const memory = await openLix();
	expect(mocks.local.mock.calls[0]?.[0]).toEqual({ kind: "memory" });
	expect(mocks.local.mock.calls[0]?.[3]).toBeUndefined();
	await memory.close();
	const local = await openLix({ storage: storage() });
	expect(mocks.local.mock.calls[1]?.[0]).toEqual({
		kind: "filesystem",
		path: "/fixture",
	});
	expect(mocks.local.mock.calls[1]?.[3]).toBeUndefined();
	expect(mocks.remote).not.toHaveBeenCalled();
	await local.close();
});

test("server defaults to remote; partial_replica explicitly opts into on-demand sync", async () => {
	const remote = await openLix({ server });
	expect(mocks.remote).toHaveBeenCalledWith(server);
	expect(mocks.local).not.toHaveBeenCalled();
	await remote.close();
	const local = await openLix({
		storage: storage(),
		server: { ...server, mode: "partial_replica" },
	});
	expect(mocks.local.mock.calls[0]?.[3]).toEqual({
		url: server.url,
		headers: undefined,
		fetch: undefined,
	});
	await local.close();
});

test.each(["sync", "replica", "unknown"])("unsupported mode %s fails before opening", async (mode) => {
	await expect(
		openLix({ server: { ...server, mode } } as never),
	).rejects.toThrow("server.mode must be");
	expect(mocks.local).not.toHaveBeenCalled();
	expect(mocks.remote).not.toHaveBeenCalled();
});

test("remote execution rejects local-only options before opening", async () => {
	await expect(
		openLix({ server, telemetry: { onSpan() {} } } as never),
	).rejects.toThrow("does not accept local telemetry or onProgress");
	await expect(openLix({ server, onProgress() {} } as never)).rejects.toThrow(
		"does not accept local telemetry or onProgress",
	);
	expect(mocks.remote).not.toHaveBeenCalled();
});

test("explicit remote opens remotely", async () => {
	const remote = await openLix({server: {...server, mode: "remote"}});
	expect(mocks.remote).toHaveBeenCalledOnce();
	expect(mocks.local).not.toHaveBeenCalled();
	await remote.close();
});
test.each([undefined, "remote"])("remote mode %s rejects storage before opening", async (mode) => {
	await expect(openLix({storage: storage(), server: {...server, mode}} as never)).rejects.toThrow('set server.mode to "partial_replica"');
	expect(mocks.local).not.toHaveBeenCalled();
	expect(mocks.remote).not.toHaveBeenCalled();
});
test("partial_replica requires storage before opening", async () => {
	await expect(openLix({server: {...server, mode: "partial_replica"}} as never)).rejects.toThrow('requires storage');
	expect(mocks.local).not.toHaveBeenCalled();
	expect(mocks.remote).not.toHaveBeenCalled();
});
