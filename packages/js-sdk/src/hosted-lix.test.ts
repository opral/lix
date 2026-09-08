import { beforeEach, expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";
import { createLix, deleteLix } from "./hosted-lix.js";

const lifecycle = vi.hoisted(() => vi.fn());
vi.mock("./worker/client.js", () => ({ hostedLixWorkerOperation: lifecycle }));
beforeEach(() => lifecycle.mockReset());

const host = {
	url: "https://example.com",
	headers: async () => ({ Authorization: "Bearer test" }),
};
const hosted = { id: "repo-id", url: "https://example.com/lix/repo-id" };

test("creation and deletion use explicit hosted operations and normalized credentials", async () => {
	lifecycle.mockResolvedValueOnce(hosted).mockResolvedValueOnce(undefined);
	expect(await createLix({ server: host })).toEqual(hosted);
	await deleteLix({ server: { ...host, url: hosted.url } });
	expect(lifecycle.mock.calls).toEqual([
		[
			{
				kind: "hosted.create",
				server: {
					url: "https://example.com/",
					headers: [["authorization", "Bearer test"]],
				},
			},
		],
		[
			{
				kind: "hosted.delete",
				server: {
					url: hosted.url,
					headers: [["authorization", "Bearer test"]],
				},
			},
		],
	]);
});

test("creation from local retains source ownership until Rust creation completes", async () => {
	let finish!: (value: typeof hosted) => void;
	const create = vi.fn(
		() =>
			new Promise<typeof hosted>((resolve) => {
				finish = resolve;
			}),
	);
	const close = vi.fn(async () => undefined);
	const local = new Lix({
		createHosted: create,
		close,
	} as unknown as LixBinding);
	const creating = createLix({ server: host, from: local });
	const closing = local.close();
	await vi.waitFor(() => expect(create).toHaveBeenCalledOnce());
	expect(close).not.toHaveBeenCalled();
	finish(hosted);
	expect(await creating).toEqual(hosted);
	await closing;
	expect(close).toHaveBeenCalledOnce();
	expect(lifecycle).not.toHaveBeenCalled();
	await expect(createLix({ server: host, from: local })).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(create).toHaveBeenCalledOnce();
});

test("cannot create from a remote-only session or a non-Lix value", async () => {
	const remote = new Lix({
		close: async () => undefined,
	} as unknown as LixBinding);
	await expect(createLix({ server: host, from: remote })).rejects.toThrow(
		"requires a local Lix",
	);
	await expect(createLix({ server: host, from: {} as Lix })).rejects.toThrow(
		"must be an open local Lix",
	);
	expect(lifecycle).not.toHaveBeenCalled();
	await remote.close();
});

test("failed credentials do not start a hosted operation", async () => {
	const failure = new Error("signed out");
	await expect(
		createLix({
			server: {
				url: host.url,
				headers: () => {
					throw failure;
				},
			},
		}),
	).rejects.toBe(failure);
	expect(lifecycle).not.toHaveBeenCalled();
});

test("idempotency keys reach empty and source creation unchanged", async () => {
	lifecycle.mockResolvedValue(hosted);
	await createLix({ server: host, idempotencyKey: "retry-42" });
	expect(lifecycle.mock.calls[0]?.[0].server.idempotencyKey).toBe("retry-42");
	const create = vi.fn(
		async (_server: import("./binding-types.js").HostedServerBindingOptions) =>
			hosted,
	);
	const local = new Lix({
		createHosted: create,
		close: async () => undefined,
	} as unknown as LixBinding);
	await createLix({ server: host, from: local, idempotencyKey: "retry-43" });
	expect(create.mock.calls[0]?.[0]).toMatchObject({
		idempotencyKey: "retry-43",
	});
	await local.close();
});

test("unsupported custom transport is rejected rather than silently ignored", async () => {
	await expect(
		createLix({ server: { url: host.url, fetch: vi.fn() } } as never),
	).rejects.toThrow("does not accept a custom fetch");
	expect(lifecycle).not.toHaveBeenCalled();
});
