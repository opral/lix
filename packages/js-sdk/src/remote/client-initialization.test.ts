import { beforeEach, expect, test, vi } from "vitest";

const wasm = vi.hoisted(() => ({
	init: vi.fn(),
	openRemote: vi.fn(),
}));

vi.mock("../wasm/lix_js_sdk.js", () => ({
	default: wasm.init,
	openRemote: wasm.openRemote,
}));

beforeEach(() => {
	vi.resetModules();
	wasm.init.mockReset();
	wasm.openRemote.mockReset();
});

test("concurrent first opens share one WASM initialization", async () => {
	const { openRemoteLixBinding } = await import("./client.js");
	let finishInitialization: (() => void) | undefined;
	wasm.init.mockReturnValue(
		new Promise<void>((resolve) => {
			finishInitialization = resolve;
		}),
	);
	wasm.openRemote.mockResolvedValue({});
	const options = {
		mode: "remote" as const,
		url: "https://lix.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: vi.fn(),
	};

	const first = openRemoteLixBinding(options);
	const second = openRemoteLixBinding(options);
	await vi.waitFor(() => expect(wasm.init).toHaveBeenCalledTimes(1));

	finishInitialization?.();
	await Promise.all([first, second]);
	expect(wasm.init).toHaveBeenCalledTimes(1);
	expect(wasm.openRemote).toHaveBeenCalledTimes(2);
});

test("an initialization failure remains the shared result", async () => {
	const { openRemoteLixBinding } = await import("./client.js");
	const failure = new Error("WASM initialization failed");
	wasm.init.mockRejectedValue(failure);
	const options = {
		mode: "remote" as const,
		url: "https://lix.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
		fetch: vi.fn(),
	};

	await expect(openRemoteLixBinding(options)).rejects.toBe(failure);
	await expect(openRemoteLixBinding(options)).rejects.toBe(failure);
	expect(wasm.init).toHaveBeenCalledTimes(1);
	expect(wasm.openRemote).not.toHaveBeenCalled();
});
