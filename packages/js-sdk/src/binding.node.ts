import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type {
	LixStorageConfig,
	LixBinding,
	PluginRuntimeDispatch,
	TelemetryDispatch,
} from "./binding-types.js";

type NativeAddon = {
	Lix: {
		openMemory(
			dispatch: PluginRuntimeDispatch,
			telemetry?: (spanJson: string) => void,
		): Promise<LixBinding>;
		openSQLite(
			path: string,
			dispatch: PluginRuntimeDispatch,
			telemetry?: (spanJson: string) => void,
		): Promise<LixBinding>;
		openLocalFilesystem(
			path: string,
			lixDir: string | undefined,
			syncAllFiles: boolean,
			dispatch: PluginRuntimeDispatch,
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

let addon: NativeAddon;
try {
	addon = require(resolveNativePath()) as NativeAddon;
} catch (cause) {
	const error = new Error(
		`Failed to load @lix-js/sdk native addon for ${process.platform}-${process.arch}. ` +
			"This package requires the matching optional native binary package. " +
			"Run `npm run build` from packages/js-sdk for local development, or install a release that includes your platform binary.",
	) as Error & { cause?: unknown };
	error.cause = cause;
	throw error;
}

export function openLixBinding(
	storage: LixStorageConfig,
	dispatch: PluginRuntimeDispatch,
	telemetry?: TelemetryDispatch,
): Promise<LixBinding> {
	const nativeTelemetry = telemetry
		? (spanJson: string) => telemetry(JSON.parse(spanJson))
		: undefined;
	switch (storage.kind) {
		case "memory":
			if (storage.snapshot !== undefined) {
				throw new Error(
					"Memory snapshots are only available in the browser binding",
				);
			}
			return addon.Lix.openMemory(dispatch, nativeTelemetry);
		case "sqlite":
			return addon.Lix.openSQLite(storage.path, dispatch, nativeTelemetry);
		case "localFilesystem":
			return addon.Lix.openLocalFilesystem(
				storage.path,
				storage.lixDir,
				storage.syncAllFiles,
				dispatch,
				nativeTelemetry,
			);
	}
}
