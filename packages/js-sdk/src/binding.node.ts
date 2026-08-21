import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type {
	LixStorageConfig,
	LixBinding,
	ObserveEventsBinding,
	SyncServerBindingOptions,
	TelemetryDispatch,
	TelemetryParentContext,
} from "./binding-types.js";

type NativeAddon = {
	Lix: {
		openMemory(
			telemetry?: (spanJson: string) => void,
			telemetryParentJson?: string,
			serverUrl?: string,
			serverHeaders?: [string, string][],
		): Promise<NativeLixBinding>;
		openFilesystemStorage(
			path: string,
			syncAllFiles: boolean,
			telemetry?: (spanJson: string) => void,
			telemetryParentJson?: string,
			serverUrl?: string,
			serverHeaders?: [string, string][],
		): Promise<NativeLixBinding>;
	};
};

type NativeObserveEventsBinding = Omit<
	ObserveEventsBinding,
	"setTelemetryParent"
> & {
	setTelemetryParent(parentJson?: string): void;
};

type NativeLixBinding = Omit<LixBinding, "observe" | "setTelemetryParent"> & {
	setTelemetryParent(parentJson?: string): void;
	observe(
		sql: Parameters<LixBinding["observe"]>[0],
		params: Parameters<LixBinding["observe"]>[1],
	): Promise<NativeObserveEventsBinding>;
};

function normalizeNativeObserveEvents(
	events: NativeObserveEventsBinding,
): ObserveEventsBinding {
	return new Proxy(events, {
		get(target, property, receiver) {
			if (property === "setTelemetryParent") {
				return (parent?: TelemetryParentContext) =>
					target.setTelemetryParent(
						parent === undefined ? undefined : JSON.stringify(parent),
					);
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as ObserveEventsBinding;
}

function normalizeNativeBinding(binding: NativeLixBinding): LixBinding {
	return new Proxy(binding, {
		get(target, property, receiver) {
			if (property === "setTelemetryParent") {
				return (parent?: TelemetryParentContext) =>
					target.setTelemetryParent(
						parent === undefined ? undefined : JSON.stringify(parent),
					);
			}
			if (property === "observe") {
				return async (
					sql: Parameters<LixBinding["observe"]>[0],
					params: Parameters<LixBinding["observe"]>[1],
				) => normalizeNativeObserveEvents(await target.observe(sql, params));
			}
			const value = Reflect.get(target, property, receiver) as unknown;
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as LixBinding;
}

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
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
): Promise<LixBinding> {
	try {
		return await openNativeLixBinding(storage, telemetry, telemetryParent, server);
	} catch (nativeError) {
		if (storage.kind !== "memory" || server !== undefined) throw nativeError;
		try {
			const { openMemoryWasmBinding } = await import(
				"./binding.node-wasm.js"
			);
			return await openMemoryWasmBinding(telemetry, telemetryParent);
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
	telemetryParent?: TelemetryParentContext,
	server?: SyncServerBindingOptions,
): Promise<LixBinding> {
	if (server?.fetch) {
		throw new TypeError("Custom sync fetch is only supported by the browser worker");
	}
	switch (storage.kind) {
		case "memory": {
			const nativeAddon = loadNativeAddon();
			const nativeTelemetry = telemetry
				? (spanJson: string) => telemetry(JSON.parse(spanJson))
				: undefined;
			if (nativeTelemetry) {
				return normalizeNativeBinding(await nativeAddon.Lix.openMemory(
					nativeTelemetry,
					telemetryParent ? JSON.stringify(telemetryParent) : undefined,
					server?.url,
					server?.headers,
				));
			}
			return normalizeNativeBinding(await nativeAddon.Lix.openMemory(
				undefined,
				undefined,
				server?.url,
				server?.headers,
			));
		}
		case "jsStorage":
			throw new Error("JavaScript storage providers are only available in browsers");
		case "filesystem": {
			const nativeAddon = loadNativeAddon();
			const nativeTelemetry = telemetry
				? (spanJson: string) => telemetry(JSON.parse(spanJson))
				: undefined;
			if (nativeTelemetry) {
				return normalizeNativeBinding(await nativeAddon.Lix.openFilesystemStorage(
					storage.path,
					storage.syncAllFiles,
					nativeTelemetry,
					telemetryParent ? JSON.stringify(telemetryParent) : undefined,
					server?.url,
					server?.headers,
				));
			}
			return normalizeNativeBinding(await nativeAddon.Lix.openFilesystemStorage(
				storage.path,
				storage.syncAllFiles,
				undefined,
				undefined,
				server?.url,
				server?.headers,
			));
		}
	}
}
