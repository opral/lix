import type {
	LixStorageConfig,
	LixBinding,
	LixStorageProviderModule,
	TelemetryDispatch,
} from "./binding-types.js";

// Generated before TypeScript compilation and emitted beside this module.
import initWasm, { openJsStorage, openMemory } from "./wasm/lix_js_sdk.js";

let wasmInitialized: Promise<unknown> | undefined;

function initializeWasm(): Promise<unknown> {
	return (wasmInitialized ??= initWasm());
}

export async function openLixBinding(
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
): Promise<LixBinding> {
	await initializeWasm();
	switch (storage.kind) {
		case "memory":
			return openMemory(telemetry) as Promise<LixBinding>;
		case "jsStorage": {
			const module = (await import(
				/* @vite-ignore */ storage.moduleUrl
			)) as unknown as LixStorageProviderModule;
			if (typeof module.createLixStorageProvider !== "function") {
				throw new TypeError(
					`Storage provider module '${storage.moduleUrl}' does not export createLixStorageProvider()`,
				);
			}
			const provider = await module.createLixStorageProvider(storage.options);
			try {
				const binding = (await openJsStorage(
					provider,
					telemetry,
				)) as unknown as LixBinding;
				return binding;
			} catch (error) {
				await provider.close().catch(() => undefined);
				throw error;
			}
		}
		case "filesystem":
			throw new Error("FilesystemStorage is only available in Node.js");
	}
}
