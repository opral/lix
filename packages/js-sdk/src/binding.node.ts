import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type {
	LixStorageConfig,
	LixBinding,
	TelemetryDispatch,
} from "./binding-types.js";

type NativeAddon = {
	Lix: {
		openMemory(
			telemetry?: (spanJson: string) => void,
		): Promise<LixBinding>;
		openFilesystemStorage(
			path: string,
			syncAllFiles: boolean,
			telemetry?: (spanJson: string) => void,
		): Promise<LixBinding>;
	};
};

const require = createRequire(import.meta.url);
const localNativePath = fileURLToPath(
	new URL("../lix_js_sdk.node", import.meta.url),
);

const nativePackages = {
	"linux-x64": "@lix-js/sdk-linux-x64",
	"linux-arm64": "@lix-js/sdk-linux-arm64",
	"darwin-arm64": "@lix-js/sdk-darwin-arm64",
	"win32-x64": "@lix-js/sdk-win32-x64",
} as const;

function resolveNativePath() {
	if (existsSync(localNativePath)) return localNativePath;
	const key =
		`${process.platform}-${process.arch}` as keyof typeof nativePackages;
	const packageName = nativePackages[key];
	let packageResolutionError: unknown;
	if (packageName) {
		try {
			return require.resolve(packageName);
		} catch (error) {
			packageResolutionError = error;
		}
	}
	if (!packageName) {
		throw new Error(`Unsupported platform ${process.platform}-${process.arch}`);
	}
	throw packageResolutionError;
}

let addon: NativeAddon | undefined;
let addonLoadError: Error | undefined;

function loadNativeAddon(): NativeAddon {
	if (addon) return addon;
	if (addonLoadError) throw addonLoadError;
	try {
		addon = require(resolveNativePath()) as NativeAddon;
		return addon;
	} catch (cause) {
		const error = new Error(
			`Failed to load @lix-js/sdk native addon for ${process.platform}-${process.arch}. ` +
				"This package requires the matching optional native binary package. " +
				"Run `npm run build` from packages/js-sdk for local development, or install a release that includes your platform binary.",
			{ cause },
		);
		addonLoadError = error;
		throw error;
	}
}

export async function openLixBinding(
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
): Promise<LixBinding> {
	try {
		return await openNativeLixBinding(storage, telemetry);
	} catch (nativeError) {
		if (storage.kind !== "memory") throw nativeError;
		try {
			const { openMemoryWasmBinding } = await import(
				"./binding.node-wasm.js"
			);
			return await openMemoryWasmBinding(telemetry);
		} catch (wasmError) {
			throw new AggregateError(
				[nativeError, wasmError],
				"Failed to open in-memory Lix with either the native or WebAssembly binding.",
			);
		}
	}
}

export async function openNativeLixBinding(
	storage: LixStorageConfig,
	telemetry?: TelemetryDispatch,
): Promise<LixBinding> {
	switch (storage.kind) {
		case "memory": {
			const nativeAddon = loadNativeAddon();
			const nativeTelemetry = telemetry
				? (spanJson: string) => telemetry(JSON.parse(spanJson))
				: undefined;
			if (nativeTelemetry) return nativeAddon.Lix.openMemory(nativeTelemetry);
			return nativeAddon.Lix.openMemory();
		}
		case "indexedDb":
			throw new Error("IndexedDbStorage is only available in browsers");
		case "filesystem": {
			const nativeAddon = loadNativeAddon();
			const nativeTelemetry = telemetry
				? (spanJson: string) => telemetry(JSON.parse(spanJson))
				: undefined;
			if (nativeTelemetry) {
				return nativeAddon.Lix.openFilesystemStorage(
					storage.path,
					storage.syncAllFiles,
					nativeTelemetry,
				);
			}
			return nativeAddon.Lix.openFilesystemStorage(
				storage.path,
				storage.syncAllFiles,
			);
		}
	}
}
