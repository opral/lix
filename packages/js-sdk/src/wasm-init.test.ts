import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { readFile } from "node:fs/promises";

const wasm = vi.hoisted(() => ({
	init: vi.fn(),
	openRemote: vi.fn(),
	openMemory: vi.fn(),
}));

vi.mock("./wasm/lix_js_sdk.js", () => ({
	default: wasm.init,
	convertJsStorageReplicaToPartial: vi.fn(),
	retryJsStorageReplicaMigrationCleanup: vi.fn(),
	inspectJsStorageRepository: vi.fn(),
	migrateJsStorageRepository: vi.fn(),
	openRemote: wasm.openRemote,
	openMemory: wasm.openMemory,
	openMemoryFromSnapshot: vi.fn(),
	openJsStorage: vi.fn(),
	openJsStorageFromSnapshot: vi.fn(),
}));

vi.mock("./wasm/lix_js_sdk_bg.asset.js", () => ({
	default: "test-wasm-sha256",
}));

vi.mock("./component-host/index.js", () => ({
	compileComponent: vi.fn(),
	initializeComponentCompiler: vi.fn(async () => {}),
}));

vi.mock("node:fs/promises", () => ({
	readFile: vi.fn(async () => new Uint8Array()),
}));

const options = {
	url: "https://lix.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
	fetch: vi.fn(),
};

const validWasm = Uint8Array.from([0, 97, 115, 109, 1, 0, 0, 0]);

function makeCacheStorage() {
	const entries = new Map<string, Response>();
	const cache = {
		match: vi.fn(async (key: string | URL | Request) => {
			const url = typeof key === "string" ? key : key instanceof URL ? key.href : key.url;
			return entries.get(url)?.clone();
		}),
		put: vi.fn(async (key: string | URL | Request, response: Response) => {
			const url = typeof key === "string" ? key : key instanceof URL ? key.href : key.url;
			entries.set(url, response.clone());
		}),
		delete: vi.fn(async (key: string | URL | Request) => {
			const url = typeof key === "string" ? key : key instanceof URL ? key.href : key.url;
			return entries.delete(url);
		}),
		keys: vi.fn(async () => [...entries.keys()].map((url) => new Request(url))),
	};
	const storage = { open: vi.fn(async () => cache) };
	return { cache, entries, storage };
}

function wasmResponse(
	bytes: Uint8Array = validWasm,
	contentType = "application/wasm",
) {
	return new Response(bytes, {
		headers: { "Content-Type": contentType },
	});
}

function cacheUrl(moduleUrl: URL) {
	const url = new URL(moduleUrl);
	url.searchParams.set("lix-wasm-sha256", "test-wasm-sha256");
	return url;
}

function useBrowserGlobals(storage = makeCacheStorage().storage) {
	vi.stubGlobal("process", { ...process, versions: {} });
	vi.stubGlobal("navigator", { locks: undefined });
	vi.stubGlobal("caches", storage);
}

beforeEach(() => {
	vi.resetModules();
	vi.clearAllMocks();
	wasm.init.mockReset();
	wasm.openRemote.mockResolvedValue({});
	wasm.openMemory.mockResolvedValue({});
});

afterEach(() => vi.unstubAllGlobals());

test.each(["node", "browser with locks", "browser without locks"])(
	"local, remote and maintenance operations share one WASM initialization: %s",
	async (environment) => {
		const { openRemoteLixBinding } = await import("./remote/client.js");
		const { openMemoryWasmBinding } = await import("./binding.node-wasm.js");
		const { openLixBinding } = await import("./binding.browser.js");
		const request = vi.fn(
			(_name: string, _options: unknown, run: () => Promise<unknown>) => run(),
		);
		const fetchWasm = vi.fn(async (_input: URL) => wasmResponse());
		if (environment !== "node") {
			useBrowserGlobals();
			vi.stubGlobal("fetch", fetchWasm);
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
		const { initializeMigration } = await import("./migration-binding.browser.js");
		const migration = initializeMigration();
		await vi.waitFor(() => expect(wasm.init).toHaveBeenCalled());
		expect(wasm.openMemory).not.toHaveBeenCalled();
		expect(wasm.openRemote).not.toHaveBeenCalled();

		initialization.resolve();
		await Promise.all([local, remote, secondRemote, migration]);
		expect(wasm.init).toHaveBeenCalledTimes(1);
		expect(wasm.openMemory).toHaveBeenCalledTimes(1);
		expect(wasm.openRemote).toHaveBeenCalledTimes(2);
		expect(readFile).toHaveBeenCalledTimes(environment === "node" ? 1 : 0);
		if (environment === "browser with locks") {
			const moduleUrl = fetchWasm.mock.calls[0]![0] as URL;
			expect(request).toHaveBeenCalledExactlyOnceWith(
				`lix:browser-wasm:${cacheUrl(moduleUrl).href}`,
				{ mode: "exclusive" },
				expect.any(Function),
			);
			expect(wasm.init.mock.calls[0]![0].module_or_path).toBeInstanceOf(
				WebAssembly.Module,
			);
		} else {
			expect(request).not.toHaveBeenCalled();
		}
	},
);

test("a verified browser module survives a new worker realm and an offline reopen", async () => {
	const { cache, entries, storage } = makeCacheStorage();
	useBrowserGlobals(storage);
	const fetchWasm = vi.fn(async (_input: URL) =>
		wasmResponse(validWasm, "application/octet-stream"),
	);
	vi.stubGlobal("fetch", fetchWasm);
	wasm.init.mockResolvedValue(undefined);

	const firstRealm = await import("./wasm-init.js");
	await firstRealm.initializeWasm();
	const moduleUrl = fetchWasm.mock.calls[0]![0] as URL;
	expect(wasm.init.mock.calls[0]![0].module_or_path).toBeInstanceOf(
		WebAssembly.Module,
	);
	expect(entries.has(cacheUrl(moduleUrl).href)).toBe(true);
	expect(cache.put).toHaveBeenCalledTimes(1);

	vi.resetModules();
	const offlineFailure = new TypeError("Failed to fetch");
	fetchWasm.mockRejectedValue(offlineFailure);
	const reopenedRealm = await import("./wasm-init.js");
	await reopenedRealm.initializeWasm();
	expect(fetchWasm).toHaveBeenCalledTimes(1);
	expect(wasm.init).toHaveBeenCalledTimes(2);
});

test("a corrupt cached response is deleted and replaced by a compiled response", async () => {
	const { cache, entries, storage } = makeCacheStorage();
	useBrowserGlobals(storage);
	const moduleUrl = new URL("./wasm/lix_js_sdk_bg.wasm", import.meta.url);
	const cacheKeyUrl = cacheUrl(moduleUrl);
	entries.set(cacheKeyUrl.href, wasmResponse(Uint8Array.from([1, 2, 3])));
	const fetchWasm = vi.fn(async (_input: URL) => wasmResponse());
	vi.stubGlobal("fetch", fetchWasm);
	wasm.init.mockResolvedValue(undefined);

	const { initializeWasm } = await import("./wasm-init.js");
	await initializeWasm();

	expect(cache.delete).toHaveBeenCalledExactlyOnceWith(cacheKeyUrl.href);
	expect(fetchWasm).toHaveBeenCalledExactlyOnceWith(moduleUrl);
	expect(cache.put).toHaveBeenCalledTimes(1);
	expect(entries.has(cacheKeyUrl.href)).toBe(true);
});

test.each(["open", "put"])(
	"CacheStorage %s failures do not prevent an online open",
	async (failedOperation) => {
		const { cache, storage } = makeCacheStorage();
		if (failedOperation === "open") {
			storage.open.mockRejectedValue(new DOMException("Storage unavailable"));
		} else {
			cache.put.mockRejectedValue(new DOMException("Quota exceeded", "QuotaExceededError"));
		}
		useBrowserGlobals(storage);
		vi.stubGlobal("fetch", vi.fn(async (_input: URL) => wasmResponse()));
		wasm.init.mockResolvedValue(undefined);

		const { initializeWasm } = await import("./wasm-init.js");
		await expect(initializeWasm()).resolves.toBeUndefined();
		expect(wasm.init).toHaveBeenCalledOnce();
	},
);

test("offline cache misses expose a safe fetch diagnosis and retain the network cause", async () => {
	const { storage } = makeCacheStorage();
	storage.open.mockRejectedValue(new DOMException("Storage unavailable"));
	useBrowserGlobals(storage);
	const networkFailure = new TypeError("Failed to fetch");
	vi.stubGlobal("fetch", vi.fn(async (_input: URL) => Promise.reject(networkFailure)));

	const { initializeWasm } = await import("./wasm-init.js");
	const error = await initializeWasm().catch((reason: unknown) => reason as Error & {
		code?: string;
		details?: Record<string, unknown>;
		cause?: unknown;
	});

	expect(error).toMatchObject({
		name: "LixError",
		code: "LIX_WASM_ASSET_UNAVAILABLE",
		details: { phase: "wasm_fetch", cacheStatus: "unavailable" },
	});
	expect(error.cause).toBe(networkFailure);
	expect(error.message).not.toContain("lix_js_sdk_bg.wasm");
	expect(JSON.stringify(error.details)).not.toContain("lix_js_sdk_bg.wasm");
});

test("online compile failures report a safe phase and cache status", async () => {
	useBrowserGlobals();
	vi.stubGlobal("fetch", vi.fn(async (_input: URL) => wasmResponse(Uint8Array.from([1, 2, 3]))));

	const { initializeWasm } = await import("./wasm-init.js");
	const error = await initializeWasm().catch((reason: unknown) => reason as Error & {
		code?: string;
		details?: Record<string, unknown>;
	});

	expect(error).toMatchObject({
		name: "LixError",
		code: "LIX_WASM_ASSET_UNAVAILABLE",
		details: { phase: "wasm_compile", cacheStatus: "miss" },
	});
	expect(error.message).not.toContain("lix_js_sdk_bg.wasm");
	expect(JSON.stringify(error.details)).not.toContain("lix_js_sdk_bg.wasm");
});

test("a failed initialization is shared, then a later call retries", async () => {
	const { openRemoteLixBinding } = await import("./remote/client.js");
	const { openMemoryWasmBinding } = await import("./binding.node-wasm.js");
	const { initializeMigration } = await import("./migration-binding.browser.js");
	const failure = new Error("WASM initialization failed");
	const firstInitialization = Promise.withResolvers<void>();
	wasm.init.mockReturnValueOnce(firstInitialization.promise).mockResolvedValueOnce(undefined);

	const local = openMemoryWasmBinding();
	const remote = openRemoteLixBinding(options);
	const maintenance = initializeMigration();
	await vi.waitFor(() => expect(wasm.init).toHaveBeenCalledOnce());
	firstInitialization.reject(failure);
	await expect(Promise.all([local, remote, maintenance])).rejects.toBe(failure);

	await expect(openRemoteLixBinding(options)).resolves.toBeDefined();
	await expect(initializeMigration()).resolves.toBeUndefined();
	expect(wasm.init).toHaveBeenCalledTimes(2);
	expect(wasm.openMemory).not.toHaveBeenCalled();
	expect(wasm.openRemote).toHaveBeenCalledOnce();
});
