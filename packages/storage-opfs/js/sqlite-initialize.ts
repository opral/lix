type SqliteInitializer<T> = (options: {
	instantiateWasm(
		imports: WebAssembly.Imports,
		onSuccess: (
			instance: WebAssembly.Instance,
			module: WebAssembly.Module,
		) => void,
	): object;
}) => Promise<T>;

/** Propagate failures from Emscripten's callback-based Wasm initialization. */
export function initializeBundledSqlite<T>(
	initialize: SqliteInitializer<T>,
	bytes: Uint8Array<ArrayBuffer>,
): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		const initialized = initialize({
			instantiateWasm(imports, onSuccess) {
				void (async () => {
					const module = await WebAssembly.compile(bytes);
					const instance = await WebAssembly.instantiate(module, imports);
					onSuccess(instance, module);
				})().catch(reject);
				return {};
			},
		});
		void initialized.then(resolve, reject);
	});
}
