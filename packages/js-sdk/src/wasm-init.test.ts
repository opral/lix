import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { readFile } from "node:fs/promises";

const wasm = vi.hoisted(() => ({
	init: vi.fn(),
	openRemote: vi.fn(),
	openMemory: vi.fn(),
}));

vi.mock("./wasm/lix_js_sdk.js", () => ({
	default: wasm.init,
	openRemote: wasm.openRemote,
	openMemory: wasm.openMemory,
	openMemoryFromSnapshot: vi.fn(),
	openJsStorage: vi.fn(),
	openJsStorageFromSnapshot: vi.fn(),
}));

vi.mock("node:fs/promises", () => ({
	readFile: vi.fn(async () => new Uint8Array()),
}));

const options = {
	url: "https://lix.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
	fetch: vi.fn(),
};

beforeEach(() => {
	vi.resetModules();
	vi.clearAllMocks();
	wasm.init.mockReset();
	wasm.openRemote.mockResolvedValue({});
	wasm.openMemory.mockResolvedValue({});
});

afterEach(() => vi.unstubAllGlobals());

test.each(["node", "browser with locks", "browser without locks"])(
	"local and remote opens share one WASM initialization: %s",
	async (environment) => {
		const { openRemoteLixBinding } = await import("./remote/client.js");
		const { openMemoryWasmBinding } = await import("./binding.node-wasm.js");
		const { openLixBinding } = await import("./binding.browser.js");
		const request = vi.fn(
			(_name: string, _options: unknown, run: () => Promise<unknown>) => run(),
		);
		if (environment !== "node") {
			vi.stubGlobal("process", { ...process, versions: {} });
			vi.stubGlobal("navigator", {
				locks: environment === "browser with locks" ? { request } : undefined,
			});
		}
		const initialization = Promise.withResolvers<void>();
		wasm.init.mockReturnValue(initialization.promise);
		const local = environment === "node"
			? openMemoryWasmBinding()
			: openLixBinding({ kind: "memory" });
		const remote = openRemoteLixBinding(options);
		const secondRemote = openRemoteLixBinding(options);
		await vi.waitFor(() => expect(wasm.init).toHaveBeenCalled());
		expect(wasm.openMemory).not.toHaveBeenCalled();
		expect(wasm.openRemote).not.toHaveBeenCalled();

		initialization.resolve();
		await Promise.all([local, remote, secondRemote]);
		expect(wasm.init).toHaveBeenCalledTimes(1);
		expect(wasm.openMemory).toHaveBeenCalledTimes(1);
		expect(wasm.openRemote).toHaveBeenCalledTimes(2);
		expect(readFile).toHaveBeenCalledTimes(environment === "node" ? 1 : 0);
		if (environment === "browser with locks") {
			const moduleUrl = wasm.init.mock.calls[0]![0].module_or_path as URL;
			expect(request).toHaveBeenCalledExactlyOnceWith(
				`lix:browser-wasm:${moduleUrl.href}`,
				{ mode: "exclusive" },
				expect.any(Function),
			);
		} else {
			expect(request).not.toHaveBeenCalled();
		}
	},
);

test("an initialization failure is shared across local and remote bindings", async () => {
	const { openRemoteLixBinding } = await import("./remote/client.js");
	const { openMemoryWasmBinding } = await import("./binding.node-wasm.js");
	const failure = new Error("WASM initialization failed");
	wasm.init.mockRejectedValue(failure);

	await expect(openRemoteLixBinding(options)).rejects.toBe(failure);
	await expect(openMemoryWasmBinding()).rejects.toBe(failure);
	await expect(openRemoteLixBinding(options)).rejects.toBe(failure);
	expect(wasm.init).toHaveBeenCalledTimes(1);
	expect(wasm.openMemory).not.toHaveBeenCalled();
	expect(wasm.openRemote).not.toHaveBeenCalled();
});
