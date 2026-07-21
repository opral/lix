import { afterEach, beforeEach, expect, test, vi } from "vitest";

const { transpileBytes } = vi.hoisted(() => ({
	transpileBytes: vi.fn(),
}));

vi.mock("#jco-transpile", () => ({ transpileBytes }));

import { createPluginRuntimeDispatch } from "./plugin-runtime.js";

const generatedModule = new TextEncoder().encode(`
	let instanceCount = 0;
	export async function instantiate(getCoreModule) {
		await getCoreModule("core.wasm");
		const instance = ++instanceCount;
		return {
			api: {
				detectChanges() { return []; },
				render() { return new Uint8Array([instance]); },
			},
		};
	}
`);
const emptyWasmModule = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);

beforeEach(() => {
	transpileBytes.mockReset();
	transpileBytes.mockResolvedValue({
		files: {
			"lix_plugin.js": generatedModule,
			"core.wasm": emptyWasmModule,
		},
	});
});

afterEach(() => vi.restoreAllMocks());

test("shares singleflight preparation while keeping plugin instances fresh", async () => {
	const compile = vi.spyOn(WebAssembly, "compile");
	const componentBytes = new Uint8Array([1, 2, 3, 4]);
	const firstRuntime = createPluginRuntimeDispatch();
	const secondRuntime = createPluginRuntimeDispatch();

	const [first, second] = await Promise.all([
		firstRuntime({ operation: "initComponent", componentBytes }),
		secondRuntime({ operation: "initComponent", componentBytes }),
	]);

	expect(first.errorMessage).toBeUndefined();
	expect(second.errorMessage).toBeUndefined();
	expect(transpileBytes).toHaveBeenCalledTimes(1);
	expect(compile).toHaveBeenCalledTimes(1);
	const renders = await Promise.all([
		firstRuntime({ operation: "render", componentId: first.componentId }),
		secondRuntime({ operation: "render", componentId: second.componentId }),
	]);
	expect(renders.map((render) => render.bytes?.[0]).sort()).toEqual([1, 2]);

});

test("retries failed preparation", async () => {
	transpileBytes.mockRejectedValueOnce(new Error("transpile failed"));
	const componentBytes = new Uint8Array([5, 6, 7, 8]);

	const first = await createPluginRuntimeDispatch()({
		operation: "initComponent",
		componentBytes,
	});
	const second = await createPluginRuntimeDispatch()({
		operation: "initComponent",
		componentBytes,
	});

	expect(first.errorMessage).toContain("transpile failed");
	expect(second.errorMessage).toBeUndefined();
	expect(transpileBytes).toHaveBeenCalledTimes(2);
});
