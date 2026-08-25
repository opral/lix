type WasmInitializer = (options: {
	module_or_path: URL;
}) => Promise<unknown>;

type LockManager = {
	request<T>(
		name: string,
		options: { mode: "exclusive" },
		callback: () => Promise<T>,
	): Promise<T>;
};

/**
 * Initializes one fingerprinted Wasm asset without racing the browser's shared
 * HTTP cache across tabs.
 *
 * Chromium can abort one consumer when separate workers cold-load the same
 * large response concurrently. The lock lasts only for the initial streaming
 * compilation. Once it releases, later workers consume the completed immutable
 * cache entry and compile independently.
 */
export function initializeBrowserWasm(
	initialize: WasmInitializer,
	moduleUrl: URL,
): Promise<unknown> {
	const lockManager = (
		globalThis.navigator as typeof globalThis.navigator & {
			locks?: LockManager;
		}
	).locks;
	const run = () => initialize({ module_or_path: moduleUrl });
	if (!lockManager) return run();
	return lockManager.request(
		`lix:browser-wasm:${moduleUrl.href}`,
		{ mode: "exclusive" },
		run,
	);
}
