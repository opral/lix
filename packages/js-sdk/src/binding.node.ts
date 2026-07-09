import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type {
	LixBackendConfig,
	LixBinding,
	PluginRuntimeDispatch,
} from "./binding-types.js";

type NativeAddon = {
	Lix: {
		openMemory(dispatch: PluginRuntimeDispatch): Promise<LixBinding>;
		openSqlite(
			path: string,
			dispatch: PluginRuntimeDispatch,
		): Promise<LixBinding>;
		openFs(
			path: string,
			lixDir: string | undefined,
			syncAllFiles: boolean,
			dispatch: PluginRuntimeDispatch,
		): Promise<LixBinding>;
	};
};

const require = createRequire(import.meta.url);
const localNativePath = fileURLToPath(new URL("../lix_js_sdk.node", import.meta.url));

const nativePackages = {
	"linux-x64": "@lix-js/sdk-linux-x64",
	"linux-arm64": "@lix-js/sdk-linux-arm64",
	"darwin-arm64": "@lix-js/sdk-darwin-arm64",
	"win32-x64": "@lix-js/sdk-win32-x64",
} as const;

function resolveNativePath() {
	if (existsSync(localNativePath)) return localNativePath;
	const key = `${process.platform}-${process.arch}` as keyof typeof nativePackages;
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
	backend: LixBackendConfig,
	dispatch: PluginRuntimeDispatch,
): Promise<LixBinding> {
	switch (backend.kind) {
		case "memory":
			return addon.Lix.openMemory(dispatch);
		case "sqlite":
			return addon.Lix.openSqlite(backend.path, dispatch);
		case "fs":
			return addon.Lix.openFs(
				backend.path,
				backend.lixDir,
				backend.syncAllFiles,
				dispatch,
			);
	}
}
